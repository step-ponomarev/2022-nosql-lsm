package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.iterator.MappedIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public final class SSTable implements Closeable {
    public static final int TOMBSTONE_TAG = -1;
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
            long sizeBytes,
            int count
    ) throws IOException {
        final Path file = path.resolve(FILE_NAME);
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
            currentOffset = flush(entry, memorySegment, currentOffset);
        }

        return positions;
    }

    @Override
    public void close() {
        index.close();
        tableMemorySegment.scope().close();
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final long size = tableMemorySegment.byteSize();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        final long fromPosition = from == null ? 0 : index.findKeyPosition(from);
        final long toPosition = to == null ? size : index.findKeyPosition(to);
        if (fromPosition == toPosition) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }
    
    private static long flush(TimestampEntry entry, MemorySegment memorySegment, long offset) {
        final MemorySegment key = entry.key();
        final long keySize = key.byteSize();

        long writeOffset = offset;
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, keySize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, keySize).copyFrom(key);
        writeOffset += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, entry.getTimestamp());
        writeOffset += Long.BYTES;

        final MemorySegment value = entry.value();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, writeOffset, TOMBSTONE_TAG);
            return writeOffset + Long.BYTES;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, valueSize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, valueSize).copyFrom(value);
        writeOffset += valueSize;

        return writeOffset;
    }
}
