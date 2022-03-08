package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {
    public static final int TOMBSTONE_TAG = -1;

    private static final String fileName = "ss.table";

    private final Path path;

    public SSTable(Path basePath) {
        path = basePath;
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(path.resolve(fileName), FileChannelUtils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();

                ByteBuffer key = entry.key();

                ByteBuffer value = entry.value() == null ? null : entry.value();
                int valueSize = value == null ? TOMBSTONE_TAG : value.remaining();

                ByteBuffer buffer = ByteBuffer.allocate(key.remaining() + Integer.BYTES * 2 + (value == null ? 0 : value.remaining()));
                buffer.put(toByteBuffer(key.remaining()));
                buffer.put(key);
                buffer.put(toByteBuffer(valueSize));
                if (value != null) {
                    buffer.put(value);
                }
                buffer.flip();

                fileChannel.write(buffer);
            }
        }
    }

    public Iterator<Entry<ByteBuffer>> get() throws IOException {
        final Path filePath = path.resolve(fileName);
        if (!filePath.toFile().exists()) {
            return Collections.emptyIterator();
        }

        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
        try (final FileChannel fileChannel = FileChannel.open(filePath, FileChannelUtils.READ_OPEN_OPTIONS)) {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            final long size = fileChannel.size();
            while (mappedByteBuffer.position() != size) {
                int keySize = mappedByteBuffer.getInt();
                final ByteBuffer key = mappedByteBuffer.slice(mappedByteBuffer.position(), keySize);
                mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                int valueSize = mappedByteBuffer.getInt();
                if (valueSize == TOMBSTONE_TAG) {
                    data.remove(key);
                    continue;
                }

                final ByteBuffer value = mappedByteBuffer.slice(mappedByteBuffer.position(), valueSize);
                mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                data.put(key, new BaseEntry<>(
                        key,
                        value
                ));
            }
        }

        return data.values().iterator();
    }

    private static ByteBuffer toByteBuffer(int num) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(num).array());
    }
}
