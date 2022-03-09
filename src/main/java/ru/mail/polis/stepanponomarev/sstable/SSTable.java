package ru.mail.polis.stepanponomarev.sstable;

import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SSTable implements Closeable {
    private static final String DATA_FILE_NAME = "ss.data";

    private final Path dataPath;
    private final FileChannel readChannel;
    private final Index index;

    public SSTable(Path path) throws IOException {
        dataPath = path.resolve(DATA_FILE_NAME);
        if (Files.notExists(dataPath)) {
            Files.createFile(dataPath);
        }

        index = new Index(path);
        readChannel = FileChannel.open(dataPath, Utils.READ_OPEN_OPTIONS);
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        final List<Integer> positionList = new ArrayList<>();
        try (FileChannel writeChannel = FileChannel.open(dataPath, Utils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();
                positionList.add((int) writeChannel.position());
                writeChannel.write(toByteBuffer(entry));
            }
        }

        index.flushIndex(positionList);
    }

    @Override
    public void close() throws IOException {
        index.close();
        readChannel.close();
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        final long size = readChannel.size();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        MappedByteBuffer mappedTable = readChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);

        return new MappedIterator(index.sliceTable(from, to, mappedTable));
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
