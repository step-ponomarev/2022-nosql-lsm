package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MappedIterator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public final class SSTable {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private static final String FILE_NAME = "sstable.data";

    private final Index index;
    private final long created;
    private final MemorySegment tableMemorySegment;

    private SSTable(Index index, MemorySegment tableMemorySegment, long created) {
        this.index = index;
        this.tableMemorySegment = tableMemorySegment;
        this.created = created;
    }

    public static SSTable createInstance(
            Path path,
            Iterator<TimestampEntry> data,
            long sizeBytes,
            int count,
            long created
    ) throws IOException {
        final Path dir = path.resolve(SSTABLE_DIR_NAME + created);
        Files.createDirectory(dir);

        final Path file = dir.resolve(FILE_NAME);
        Files.createFile(file);

        final long fileSize = (long) Long.BYTES * 2 * count + sizeBytes;
        final long[] positions = flushAndAndGetPositions(file, data, fileSize, count);
        final MemorySegment tableMemorySegment = MemorySegment.mapFile(
                file,
                0,
                fileSize,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new SSTable(
                Index.createInstance(dir, positions, tableMemorySegment),
                tableMemorySegment,
                created
        );
    }

    public static List<SSTable> wakeUpSSTables(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            final List<String> tableDirNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .sorted()
                    .toList();

            final List<SSTable> tables = new ArrayList<>(tableDirNames.size());
            for (String name : tableDirNames) {
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }

    public long getCreated() {
        return created;
    }

    public static SSTable upInstance(Path path) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("File" + path + " is not exits.");
        }

        final MemorySegment memorySegment = MemorySegment.mapFile(
                file,
                0,
                Files.size(file),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        final Index index = Index.upInstance(path, memorySegment);
        return new SSTable(index, memorySegment, getCreatedTime(file));
    }

    private static long getCreatedTime(Path file) {
        final String pathStr = file.toString();
        final String lastPathPart = pathStr.substring(pathStr.indexOf(SSTABLE_DIR_NAME));

        return Long.parseLong(
                lastPathPart.substring(lastPathPart.indexOf("_") + 1, lastPathPart.indexOf("/"))
        );
    }

    private static long[] flushAndAndGetPositions(
            Path file,
            Iterator<TimestampEntry> data,
            long fileSize,
            int dataAmount
    ) throws IOException {
        final MemorySegment memorySegment = MemorySegment.mapFile(
                file,
                0,
                fileSize,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        int i = 0;
        final long[] positions = new long[dataAmount];

        long currentOffset = 0;
        while (data.hasNext()) {
            positions[i++] = currentOffset;

            final TimestampEntry entry = data.next();
            currentOffset = Utils.flush(entry, memorySegment, currentOffset);
        }

        return positions;
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        final long size = tableMemorySegment.byteSize();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        final long fromPosition = from == null ? 0 : index.findKeyPositionOrNear(from);
        final long toPosition = to == null ? size : index.findKeyPositionOrNear(to);
        if (fromPosition == toPosition) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(tableMemorySegment.asSlice(0, toPosition - fromPosition));
    }
}
