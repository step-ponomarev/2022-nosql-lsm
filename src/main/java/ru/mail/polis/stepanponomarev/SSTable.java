package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable {
    private static final String fileName = "ss.table";

    private final Path path;

    public SSTable(Path basePath) {
        path = basePath;
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(getFilePath(), FileChannelUtils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();

                ByteBuffer key = entry.key();
                int keySize = key.remaining();

                fileChannel.write(Utils.toByteBuffer(keySize));
                fileChannel.write(key);

                ByteBuffer value = entry.value() == null ? null : entry.value();
                int valueSize = value == null ? Utils.TOMBSTONE_TAG : value.remaining();

                fileChannel.write(Utils.toByteBuffer(valueSize));
                if (value == null) {
                    continue;
                }
                fileChannel.write(value);
            }
        }
    }

    public Iterator<Entry<ByteBuffer>> get() throws IOException {
        final Path filePath = getFilePath();

        if (!filePath.toFile().exists()) {
            return Collections.emptyIterator();
        }

        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new TreeMap<>();

        try (final FileChannel fileChannel = FileChannel.open(filePath, FileChannelUtils.READ_OPEN_OPTIONS)) {
            final MemorySegment memorySegment = MemorySegment.mapFile(filePath, 0, fileChannel.size(), FileChannel.MapMode.READ_ONLY, ResourceScope.newSharedScope());
            final long size = memorySegment.byteSize();

            long position = 0;
            while (position != size) {
                int keySize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                ByteBuffer key = memorySegment.asSlice(position, keySize).asByteBuffer();
                position += keySize;

                int valueSize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                if (valueSize == Utils.TOMBSTONE_TAG) {
                    data.remove(key);
                    continue;
                }

                ByteBuffer value = memorySegment.asSlice(position, valueSize).asByteBuffer();
                position += valueSize;

                data.put(key, new BaseEntry<>(
                        key,
                        value
                ));
            }
        }

        return data.values().iterator();
    }

    private Path getFilePath() {
        return new File(path.toAbsolutePath() + "/" + fileName).toPath();
    }
}
