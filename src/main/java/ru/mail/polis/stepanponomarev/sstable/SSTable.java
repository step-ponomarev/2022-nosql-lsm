package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class SSTable {
    public static final int TOMBSTONE_TAG = -1;

    private static final String FILE_NAME = "ss.data";

    private final MemorySegment memorySegment;
    private final Index index;
    private final long sizeBytes;

    private SSTable(Path path) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(file)) {
            throw new IllegalStateException("File should exists " + file);
        }

        sizeBytes = Files.size(file);
        memorySegment = MemorySegment.mapFile(
                file,
                0,
                sizeBytes,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope()
        );
        index = new Index(path, memorySegment);
    }

    private SSTable(Path path, Iterator<Entry<OSXMemorySegment>> data, long size) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(file)) {
            Files.createFile(file);
        }

        final Collection<Long> positions = flushAndAndGetPositions(file, data, size);

        sizeBytes = size;
        memorySegment = MemorySegment.mapFile(
                file,
                0,
                sizeBytes,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope()
        );

        index = new Index(path, positions, memorySegment);
    }

    public static SSTable upInstance(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Directory" + path + " is not exits.");
        }

        return new SSTable(path);
    }

    public static SSTable createInstance(
            Path path,
            Iterator<Entry<OSXMemorySegment>> data,
            long size
    ) throws IOException {
        return new SSTable(path, data, size);
    }

    private static Collection<Long> flushAndAndGetPositions(
            Path file,
            Iterator<Entry<OSXMemorySegment>> data,
            long sizeBytes
    ) throws IOException {
        final List<Long> positionList = new ArrayList<>();
        MemorySegment memorySegment = MemorySegment.mapFile(
                file,
                0,
                sizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.globalScope()
        );

        long currentOffset = 0;
        while (data.hasNext()) {
            positionList.add(currentOffset);

            final Entry<OSXMemorySegment> entry = data.next();
            final MemorySegment key = entry.key().getMemorySegment();
            final long keySize = key.byteSize();
            MemoryAccess.setLongAtOffset(memorySegment, currentOffset, keySize);
            currentOffset += Long.BYTES;

            memorySegment.asSlice(currentOffset, keySize).copyFrom(key);
            currentOffset += keySize;

            final OSXMemorySegment value = entry.value();
            if (value == null) {
                MemoryAccess.setLongAtOffset(memorySegment, currentOffset, TOMBSTONE_TAG);
                currentOffset += Long.BYTES;
                continue;
            }

            final long valueSize = value.getMemorySegment().byteSize();
            MemoryAccess.setLongAtOffset(memorySegment, currentOffset, valueSize);
            currentOffset += Long.BYTES;

            memorySegment.asSlice(currentOffset, valueSize).copyFrom(value.getMemorySegment());
            currentOffset += valueSize;
        }

        return positionList;
    }

    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        if (sizeBytes == 0) {
            return Collections.emptyIterator();
        }

        final long fromPosition = getKeyPositionOrDefault(from, 0);
        final long toPosition = getKeyPositionOrDefault(to, sizeBytes);

        return new MappedIterator(memorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }

    private long getKeyPositionOrDefault(OSXMemorySegment key, long defaultPosition) {
        final long keyPosition = index.getKeyPosition(key);
        if (keyPosition == -1) {
            return defaultPosition;
        }

        return keyPosition;
    }
}
