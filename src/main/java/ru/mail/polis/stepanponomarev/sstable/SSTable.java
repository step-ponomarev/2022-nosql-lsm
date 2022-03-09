package ru.mail.polis.stepanponomarev.sstable;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class SSTable {
    private static final String FILE_NAME = "ss.data";

    private final MappedByteBuffer mappedTable;
    private final Index index;

    private SSTable(Path path) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(file)) {
            throw new IllegalStateException("File should exists " + file);
        }

        final FileChannel open = FileChannel.open(file, Utils.READ_OPEN_OPTIONS);
        mappedTable = open.map(FileChannel.MapMode.READ_ONLY, 0, open.size());
        index = new Index(path, mappedTable);
    }

    private SSTable(Path path, Iterator<Entry<ByteBuffer>> data) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        Files.createFile(file);

        final Collection<Integer> positions = flushAndAndGetPositions(file, data);
        final FileChannel open = FileChannel.open(file, Utils.READ_OPEN_OPTIONS);

        mappedTable = open.map(FileChannel.MapMode.READ_ONLY, 0, open.size());
        index = new Index(path, positions, mappedTable);
    }

    public static SSTable upInstance(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Directory" + path + " is not exits.");
        }

        return new SSTable(path);
    }

    public static SSTable createInstance(Path path, Iterator<Entry<ByteBuffer>> data) throws IOException {
        return new SSTable(path, data);
    }

    private static Collection<Integer> flushAndAndGetPositions(
            Path file,
            Iterator<Entry<ByteBuffer>> data
    ) throws IOException {
        final List<Integer> positionList = new ArrayList<>();
        try (FileChannel writeChannel = FileChannel.open(file, Utils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();
                positionList.add((int) writeChannel.position());
                writeChannel.write(toByteBuffer(entry));
            }
        }

        return positionList;
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        final int size = mappedTable.limit();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        final int fromPosition = getKeyPositionOrDefault(from, 0);
        final int toPosition = getKeyPositionOrDefault(to, size);

        return new MappedIterator(
                mappedTable.slice(fromPosition, toPosition - fromPosition)
        );
    }

    private int getKeyPositionOrDefault(ByteBuffer key, int defaultPosition) {
        final int keyPosition = index.getKeyPosition(key);
        if (keyPosition == -1) {
            return defaultPosition;
        }

        return keyPosition;
    }

    private static ByteBuffer toByteBuffer(Entry<ByteBuffer> entry) {
        final ByteBuffer key = entry.key();
        final ByteBuffer value = entry.value() == null ? null : entry.value();
        final int valueSize = value == null ? Utils.TOMBSTONE_TAG : value.remaining();

        final ByteBuffer buffer = ByteBuffer.allocate(
                key.remaining() + Integer.BYTES * 2 + (value == null ? 0 : value.remaining())
        );

        buffer.put(toByteBuffer(key.remaining()));
        buffer.put(key);
        buffer.put(toByteBuffer(valueSize));
        if (value != null) {
            buffer.put(value);
        }
        buffer.flip();

        return buffer;
    }

    private static ByteBuffer toByteBuffer(int num) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(num).array());
    }
}
