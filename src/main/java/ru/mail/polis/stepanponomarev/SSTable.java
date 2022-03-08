package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SSTable {
    public static final int TOMBSTONE_TAG = -1;

    private static final String dataFileName = "ss.data";
    private static final String indexFileName = "ss.index";

    private final Path path;

    public SSTable(Path basePath) throws IOException {
        path = basePath;
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        final SortedMap<ByteBuffer, Integer> memIndex = new TreeMap<>();

        try (final FileChannel fileChannel = FileChannel.open(path.resolve(dataFileName), FileChannelUtils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();

                ByteBuffer key = entry.key();
                //TODO: косяк...
                memIndex.put(key, (int) fileChannel.position());

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

        flushMemIndex(memIndex);
    }

    private void flushMemIndex(SortedMap<ByteBuffer, Integer> index) throws IOException {
        SortedMap<ByteBuffer, Integer> byteBufferIntegerSortedMap = loadMemIndex();
        byteBufferIntegerSortedMap.putAll(index);

        Path fileIndexPath = path.resolve(indexFileName);
        Files.deleteIfExists(fileIndexPath);

        final ByteBuffer indexes = ByteBuffer.allocate(byteBufferIntegerSortedMap.values().size() * Integer.BYTES);
        for (int pos : byteBufferIntegerSortedMap.values()) {
            indexes.putInt(pos);
        }
        indexes.flip();

        try (final FileChannel indexFileChannel = FileChannel.open(fileIndexPath, FileChannelUtils.WRITE_OPEN_OPTIONS)) {
            indexFileChannel.write(indexes);
        }
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        final Path dataFilePath = path.resolve(dataFileName);
        final Path indexFilePath = path.resolve(indexFileName);
        if (!dataFilePath.toFile().exists() || !indexFilePath.toFile().exists()) {
            return Collections.emptyIterator();
        }

        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new TreeMap<>();
        try (final FileChannel fileChannel = FileChannel.open(dataFilePath, FileChannelUtils.READ_OPEN_OPTIONS);
             final FileChannel indexFileChannel = FileChannel.open(path.resolve(indexFileName), FileChannelUtils.READ_OPEN_OPTIONS)
        ) {
            MappedByteBuffer dataMappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            MappedByteBuffer indexMappedBuffer = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexFileChannel.size());

            final long size = indexFileChannel.size();
            while (indexMappedBuffer.position() != size) {
                int keyPosition = indexMappedBuffer.getInt();
                int keySize = dataMappedBuffer.position(keyPosition).getInt();

                ByteBuffer key = dataMappedBuffer.slice(dataMappedBuffer.position(), keySize);
                dataMappedBuffer.position(dataMappedBuffer.position() + keySize);
                if (!isBetween(from, key, to)) {
                    continue;
                }

                int valueSize = dataMappedBuffer.getInt();
                ByteBuffer value = dataMappedBuffer.slice(dataMappedBuffer.position(), valueSize);
                dataMappedBuffer.position(dataMappedBuffer.position() + valueSize);

                data.put(key, new BaseEntry<>(key, value));
            }
        }

        return data.values().iterator();
    }

    private boolean isBetween(ByteBuffer from, ByteBuffer key, ByteBuffer to) {
        if (from == null && to == null) {
            return true;
        }

        if (from == null) {
            return key.compareTo(to) < 0;
        }

        if (to == null) {
            return key.compareTo(from) >= 0;
        }

        return key.compareTo(from) >= 0 && key.compareTo(to) < 0;
    }

    private SortedMap<ByteBuffer, Integer> loadMemIndex() throws IOException {
        final Path indexFilePath = path.resolve(indexFileName);
        final Path dataFilePath = path.resolve(dataFileName);
        if (Files.notExists(indexFilePath) || Files.notExists(dataFilePath)) {
            return new TreeMap<>();
        }

        final SortedMap<ByteBuffer, Integer> indexMap = new TreeMap<>();
        try (
                final FileChannel indexFileChannel = FileChannel.open(indexFilePath, FileChannelUtils.READ_OPEN_OPTIONS);
                final FileChannel dataFileChannel = FileChannel.open(dataFilePath, FileChannelUtils.READ_OPEN_OPTIONS)
        ) {
            MappedByteBuffer indexMappedBuffer = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexFileChannel.size());
            MappedByteBuffer fileMappedBuffer = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFileChannel.size());

            final long size = indexFileChannel.size();
            while (indexMappedBuffer.position() < size) {
                int keyPosition = indexMappedBuffer.getInt();

                int keySize = fileMappedBuffer.position(keyPosition).getInt();
                MappedByteBuffer key = fileMappedBuffer.slice(fileMappedBuffer.position(), keySize);
                key.flip();

                indexMap.put(key, keyPosition);
            }
        }

        return indexMap;
    }

    private static ByteBuffer toByteBuffer(int num) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(num).array());
    }
}
