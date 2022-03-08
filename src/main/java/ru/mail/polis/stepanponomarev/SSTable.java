package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable {
    public static final int TOMBSTONE_TAG = -1;

    private static final String DATA_FILE_NAME = "ss.data";
    private static final String INDEX_FILE_NAME = "ss.index";

    private static final EnumSet<StandardOpenOption> APPEND_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
    );

    private static final EnumSet<StandardOpenOption> WRITE_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
    );

    private static final EnumSet<StandardOpenOption> READ_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.READ
    );

    private final Path path;

    public SSTable(Path basePath) {
        path = basePath;
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        final SortedMap<ByteBuffer, Integer> updatedIndex = new TreeMap<>();
        try (FileChannel fileChannel = FileChannel.open(path.resolve(DATA_FILE_NAME), APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();
                //TODO: косяк...
                updatedIndex.put(entry.key(), (int) fileChannel.position());
                fileChannel.write(toByteBuffer(entry));
            }
        }

        final SortedMap<ByteBuffer, Integer> index = loadIndex();
        index.putAll(updatedIndex);

        rewriteIndex(index.values());
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        final Path dataPath = path.resolve(DATA_FILE_NAME);
        final Path indexPath = path.resolve(INDEX_FILE_NAME);
        if (!dataPath.toFile().exists() || !indexPath.toFile().exists()) {
            return Collections.emptyIterator();
        }

        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new TreeMap<>();
        try (FileChannel dataFC = FileChannel.open(dataPath, READ_OPEN_OPTIONS);
             FileChannel indexFC = FileChannel.open(indexPath, READ_OPEN_OPTIONS)
        ) {
            final MappedByteBuffer dataMB = dataFC.map(FileChannel.MapMode.READ_ONLY, 0, dataFC.size());
            final MappedByteBuffer indexMB = indexFC.map(FileChannel.MapMode.READ_ONLY, 0, indexFC.size());

            final long size = indexFC.size();
            while (indexMB.position() != size) {
                int keyPosition = indexMB.getInt();
                int keySize = dataMB.position(keyPosition).getInt();

                ByteBuffer key = dataMB.slice(dataMB.position(), keySize);
                dataMB.position(dataMB.position() + keySize);
                if (!isSymmetricValues(from, key, to)) {
                    continue;
                }

                int valueSize = dataMB.getInt();
                ByteBuffer value = dataMB.slice(dataMB.position(), valueSize);
                dataMB.position(dataMB.position() + valueSize);

                data.put(key, new BaseEntry<>(key, value));
            }
        }

        return data.values().iterator();
    }

    private void rewriteIndex(Collection<Integer> positions) throws IOException {
        //TODO: Костыль чтобы не делать кучу SSTable
        final Path fileIndexPath = path.resolve(INDEX_FILE_NAME);
        Files.deleteIfExists(fileIndexPath);

        final ByteBuffer buffer = toByteBuffer(positions);
        try (FileChannel indexFC = FileChannel.open(fileIndexPath, WRITE_OPEN_OPTIONS)) {
            indexFC.write(buffer);
        }
    }

    private boolean isSymmetricValues(ByteBuffer from, ByteBuffer key, ByteBuffer to) {
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

    private SortedMap<ByteBuffer, Integer> loadIndex() throws IOException {
        final Path indexFilePath = path.resolve(INDEX_FILE_NAME);
        final Path dataFilePath = path.resolve(DATA_FILE_NAME);
        if (Files.notExists(indexFilePath) || Files.notExists(dataFilePath)) {
            return new TreeMap<>();
        }

        final SortedMap<ByteBuffer, Integer> index = new TreeMap<>();
        try (FileChannel indexFC = FileChannel.open(indexFilePath, READ_OPEN_OPTIONS);
             FileChannel dataFC = FileChannel.open(dataFilePath, READ_OPEN_OPTIONS)
        ) {
            MappedByteBuffer indexMB = indexFC.map(FileChannel.MapMode.READ_ONLY, 0, indexFC.size());
            MappedByteBuffer fileMB = dataFC.map(FileChannel.MapMode.READ_ONLY, 0, dataFC.size());

            final long size = indexFC.size();
            while (indexMB.position() < size) {
                int keyPosition = indexMB.getInt();

                int keySize = fileMB.position(keyPosition).getInt();
                ByteBuffer key = fileMB.slice(fileMB.position(), keySize);
                key.flip();

                index.put(key, keyPosition);
            }
        }

        return index;
    }

    private static ByteBuffer toByteBuffer(Entry<ByteBuffer> entry) {
        final ByteBuffer key = entry.key();
        final ByteBuffer value = entry.value() == null ? null : entry.value();
        final int valueSize = value == null ? TOMBSTONE_TAG : value.remaining();

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

    private static ByteBuffer toByteBuffer(Collection<Integer> positions) {
        final ByteBuffer buffer = ByteBuffer.allocate(positions.size() * Integer.BYTES);
        for (int pos : positions) {
            buffer.putInt(pos);
        }
        buffer.flip();

        return buffer;
    }

    private static ByteBuffer toByteBuffer(int num) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(num).array());
    }
}
