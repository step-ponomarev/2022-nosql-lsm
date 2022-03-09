package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

final class Index {
    private static final String INDEX_FILE_NAME = "ss.index";

    private final MemorySegment tableMemorySegment;
    private final MemorySegment indexMemorySegment;
    private final long sizeBytes;

    public Index(Path path, MemorySegment mappedTable) throws IOException {
        final Path file = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(file)) {
            throw new IllegalStateException("File should exists " + file);
        }

        this.sizeBytes = Files.size(file);
        this.indexMemorySegment = MemorySegment.mapFile(
                file,
                0,
                this.sizeBytes,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope()
        );
        this.tableMemorySegment = mappedTable;
    }

    public Index(Path path, Collection<Long> position, MemorySegment mappedTable) throws IOException {
        final Path file = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(file)) {
            Files.createFile(file);
        }

        flush(file, position);

        this.sizeBytes = (long) position.size() * Long.BYTES;
        this.tableMemorySegment = mappedTable;
        this.indexMemorySegment = MemorySegment.mapFile(
                file,
                0,
                this.sizeBytes,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope()
        );
    }

    private static void flush(Path file, Collection<Long> positions) throws IOException {
        final MemorySegment memorySegment = MemorySegment.mapFile(file,
                0,
                (long) positions.size() * Long.BYTES,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.globalScope()
        );

        long offset = 0;
        for (long pos : positions) {
            MemoryAccess.setLongAtOffset(memorySegment, offset, pos);
            offset += Long.BYTES;
        }

        memorySegment.force();
    }

    public long getKeyPosition(OSXMemorySegment key) {
        if (key == null) {
            return -1;
        }

        long left = 0;
        long right = sizeBytes / Long.BYTES;
        while (right >= left) {
            final long mid = left + (right - left) / 2;

            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid);
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);

            final MemorySegment foundKey = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);
            final int compareResult = key.compareTo(new OSXMemorySegment(foundKey));

            if (compareResult == 0) {
                return keyPosition;
            }

            if (compareResult < 0) {
                right = mid - 1;
            }

            if (compareResult > 0) {
                left = mid + 1;
            }
        }

        return -1;
    }
}
