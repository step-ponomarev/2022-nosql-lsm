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

    private static final EnumSet<StandardOpenOption> READ_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.READ
    );

    private final Path indexPath;
    private final Path dataPath;

    public SSTable(Path path) throws IOException {
        indexPath = path.resolve(INDEX_FILE_NAME);
        dataPath = path.resolve(DATA_FILE_NAME);

        if (Files.notExists(indexPath) || Files.notExists(dataPath)) {
            Files.createFile(indexPath);
            Files.createFile(dataPath);
        }
    }

    public void flush(Iterator<Entry<ByteBuffer>> data) throws IOException {
        final SortedMap<ByteBuffer, Integer> indexMap = new TreeMap<>();
        try (FileChannel fileChannel = FileChannel.open(dataPath, APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final Entry<ByteBuffer> entry = data.next();
                //TODO: косяк...
                indexMap.put(entry.key(), (int) fileChannel.position());
                fileChannel.write(toByteBuffer(entry));
            }
        }

        flushIndex(indexMap.values());
    }

    //TODO: Бинарный поиск допилить
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        if (Files.size(indexPath) == 0) {
            return Collections.emptyIterator();
        }

        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new TreeMap<>();
        try (FileChannel dataFC = FileChannel.open(dataPath, READ_OPEN_OPTIONS);
             FileChannel indexFC = FileChannel.open(indexPath, READ_OPEN_OPTIONS)
        ) {
            final MappedByteBuffer dataMB = dataFC.map(FileChannel.MapMode.READ_ONLY, 0, dataFC.size());
            final MappedByteBuffer indexMB = indexFC.map(FileChannel.MapMode.READ_ONLY, 0, indexFC.size());

            final long size = indexMB.remaining();
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

    private void flushIndex(Collection<Integer> positions) throws IOException {
        final ByteBuffer buffer = toByteBuffer(positions);
        try (FileChannel indexFC = FileChannel.open(indexPath, APPEND_OPEN_OPTIONS)) {
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
