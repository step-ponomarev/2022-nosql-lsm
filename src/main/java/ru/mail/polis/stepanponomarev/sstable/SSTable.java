package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public final class SSTable implements Closeable {
    public static final long TOMBSTONE_TAG = -1;
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
        final Path sstableFile = path.resolve(FILE_NAME);
        Files.createFile(sstableFile);

        final long sstableSizeBytes = (long) Long.BYTES * 2 * count + sizeBytes;
        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                sstableFile,
                0,
                sstableSizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        final Path indexFile = path.resolve(Index.FILE_NAME);
        Files.createFile(indexFile);

        final long indexSizeBytes = (long) Long.BYTES * count;
        final MemorySegment mappedIndex = MemorySegment.mapFile(
                indexFile,
                0,
                indexSizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        flush(data, mappedSsTable, mappedIndex);

        return new SSTable(new Index(mappedIndex.asReadOnly()), mappedSsTable.asReadOnly());
    }

    public static SSTable upInstance(Path path) throws IOException {
        final Path sstableFile = path.resolve(FILE_NAME);
        final Path indexFile = path.resolve(Index.FILE_NAME);
        if (Files.notExists(path) || Files.notExists(indexFile)) {
            throw new IllegalArgumentException("Files must exist.");
        }

        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                sstableFile,
                0,
                Files.size(sstableFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        final MemorySegment mappedIndex = MemorySegment.mapFile(
                indexFile,
                0,
                Files.size(indexFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new SSTable(new Index(mappedIndex), mappedSsTable);
    }

    private static void flush(Iterator<TimestampEntry> data, MemorySegment sstable, MemorySegment index) {
        long indexOffset = 0;
        long sstableOffset = 0;
        while (data.hasNext()) {
            MemoryAccess.setLongAtOffset(index, indexOffset, sstableOffset);
            indexOffset += Long.BYTES;

            final TimestampEntry entry = data.next();
            sstableOffset += flush(entry, sstable, sstableOffset);
        }
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

        if (from == null && to == null) {
            return new MappedIterator(tableMemorySegment.asSlice(0, size));
        }

        final int indexAmount = index.getPositionAmount();
        int currIndex = from == null ? 0 : findIndex(from);
        final long fromPosition;
        if (currIndex < 0) {
            currIndex = indexAmount + currIndex <= 0 ? 0 : indexAmount + currIndex;
            fromPosition = index.getPositionByIndex(currIndex);
        } else {
            fromPosition = index.getPositionByIndex(currIndex);
        }


        currIndex = to == null ? indexAmount - 1 : findIndex(to);
        final long toPosition;
        if (currIndex < 0) {
            toPosition = index.getPositionByIndex(indexAmount + currIndex - 1);
        } else {
            toPosition = currIndex == indexAmount - 1 ? size : index.getPositionByIndex(currIndex + 1);
        }

        return new MappedIterator(tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }

    private int findIndex(MemorySegment key) {
        int low = 0;
        int high = index.getPositionAmount() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            final long keyPosition = index.getPositionByIndex(mid);
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);
            final MemorySegment foundKey = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);

            final int compareResult = Utils.compare(key, foundKey);
            if (compareResult < 0) {
                low = mid + 1;
            } else if (compareResult > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -(low + 1);
    }

    /**
     * @return размер записи.
     */
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
            return writeOffset + Long.BYTES - offset;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, valueSize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, valueSize).copyFrom(value);
        writeOffset += valueSize;

        return writeOffset - offset;
    }
}
