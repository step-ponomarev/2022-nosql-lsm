package ru.mail.polis.stepanponomarev.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

final class Index {
    private static final String INDEX_FILE_NAME = "ss.index";

    private final MappedByteBuffer mappedTable;
    private final MappedByteBuffer mappedIndex;

    public Index(Path path, MappedByteBuffer mappedTable) throws IOException {
        final Path file = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(file)) {
            throw new IllegalStateException("File should exists " + file);
        }

        FileChannel fileChannel = FileChannel.open(file, Utils.READ_OPEN_OPTIONS);
        this.mappedIndex = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        this.mappedTable = mappedTable;
    }

    public Index(Path path, Collection<Integer> position, MappedByteBuffer mappedTable) throws IOException {
        final Path file = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(file)) {
            Files.createFile(file);
        }

        flush(file, position);
        FileChannel fileChannel = FileChannel.open(file, Utils.READ_OPEN_OPTIONS);

        this.mappedTable = mappedTable;
        this.mappedIndex = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
    }

    private static ByteBuffer toByteBuffer(Collection<Integer> positions) {
        final ByteBuffer buffer = ByteBuffer.allocate(positions.size() * Integer.BYTES);
        for (int pos : positions) {
            buffer.putInt(pos);
        }
        buffer.flip();

        return buffer;
    }

    private static void flush(Path file, Collection<Integer> positions) throws IOException {
        final ByteBuffer buffer = toByteBuffer(positions);
        try (FileChannel indexFC = FileChannel.open(file, Utils.APPEND_OPEN_OPTIONS)) {
            indexFC.write(buffer);
        }
    }

    public int getKeyPosition(ByteBuffer key) {
        if (key == null) {
            return -1;
        }

        return findKeyPosition(key);
    }

    private int findKeyPosition(ByteBuffer key) {
        if (key == null) {
            return -1;
        }

        int left = 0;
        int right = mappedIndex.position(0).limit();
        while (right >= left) {
            final int mid = getMidPosition(left, right);
            final int keyPosition = mappedIndex.position(mid).getInt();
            final int keySize = mappedTable.position(keyPosition).getInt();

            final ByteBuffer foundKey = mappedTable.slice(mappedTable.position(), keySize);
            final int compareResult = key.compareTo(foundKey);

            if (compareResult == 0) {
                return keyPosition;
            }

            if (compareResult < 0) {
                right = mid - Integer.BYTES;
            }

            if (compareResult > 0) {
                left = mid + Integer.BYTES;
            }
        }

        return -1;
    }

    private int getMidPosition(int left, int right) {
        int recordAmount = (right - left) / Integer.BYTES;

        return left + (recordAmount / 2) * Integer.BYTES;
    }
}
