package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MappedIterator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public final class SSTable {
    private static final String FILE_NAME = "sstable.data";

    private final Index index;
    private final MemorySegment tableMemorySegment;

    private SSTable(Index index, MemorySegment tableMemorySegment) {
        this.index = index;
        this.tableMemorySegment = tableMemorySegment;
    }

    public static SSTable createInstance(
            Path path,
            Iterator<TimestampEntry> data,
            long dataSize,
            int dataAmount
    ) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        Files.createFile(file);

        final long fileSize = (long) Long.BYTES * 2 * dataAmount + dataSize;
        final long[] positions = flushAndAndGetPositions(file, data, fileSize, dataAmount);
        final MemorySegment tableMemorySegment = MemorySegment.mapFile(
                file,
                0,
                fileSize,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newConfinedScope()
        );

        return new SSTable(
                Index.createInstance(path, positions, tableMemorySegment),
                tableMemorySegment
        );
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

        return new SSTable(index, memorySegment);
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

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final long size = tableMemorySegment.byteSize();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        final long fromPosition = index.getKeyPosition(from);
        if (fromPosition == -1) {
            return Collections.emptyIterator();
        }

        long toPosition = index.getKeyPosition(to);
        if (toPosition == -1) {
            toPosition = size;
        }

        return new MappedIterator(tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }
}
