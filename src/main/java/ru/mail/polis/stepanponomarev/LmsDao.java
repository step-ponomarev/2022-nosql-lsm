package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class LmsDao implements Dao<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> {
    private static final long maxSizeByte = 50_000;

    private final Logger logger;
    private final SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> memTable;

    private long currentSize;

    public LmsDao(Path path) throws IOException {
        logger = new Logger(path);
        memTable = createMemTable();
        currentSize = sizeOf(memTable.values());
    }

    @Override
    public Iterator<Entry<ComparableMemorySegmentWrapper>> get(ComparableMemorySegmentWrapper from, ComparableMemorySegmentWrapper to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        }

        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }

        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }

        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<ComparableMemorySegmentWrapper> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        memTable.put(entry.key(), entry);
        try {
            logger.append(entry.key().getMemorySegment().asByteBuffer(), entry.value().getMemorySegment().asByteBuffer());

            currentSize += sizeOf(entry);
            if (currentSize >= maxSizeByte) {
                logger.clean();
                currentSize = 0;
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        logger.close();
    }

    private SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> createMemTable() throws IOException {
        final Map<ByteBuffer, ByteBuffer> loadedData = logger.read();
        if (loadedData.isEmpty()) {
            return new ConcurrentSkipListMap<>();
        }

        return loadedData.entrySet()
                .stream()
                .map(this::convert)
                .collect(Collectors.toMap(Entry::key, e-> e, (o1, o2) -> o1, ConcurrentSkipListMap::new));
    }

    private long sizeOf(Collection<Entry<ComparableMemorySegmentWrapper>> values) {
        return values.stream().mapToLong(this::sizeOf).count();
    }

    private long sizeOf(Entry<ComparableMemorySegmentWrapper> entry) {
        long keySize = entry.key().getMemorySegment().byteSize();
        long valueSize = (entry.value() == null) ? 0 : entry.value().getMemorySegment().byteSize();


        return keySize + valueSize;
    }

    private Entry<ComparableMemorySegmentWrapper> convert(Map.Entry<ByteBuffer, ByteBuffer> data) {
        ByteBuffer key = data.getKey();
        ByteBuffer value = data.getValue();

        return new BaseEntry<>(
                new ComparableMemorySegmentWrapper(MemorySegment.ofByteBuffer(key)),
                new ComparableMemorySegmentWrapper(MemorySegment.ofByteBuffer(value))
        );
    }
}
