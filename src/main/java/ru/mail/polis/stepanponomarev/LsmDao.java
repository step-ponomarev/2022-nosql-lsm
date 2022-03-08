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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class LsmDao implements Dao<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> {
    private final Logger logger;
    private final SSTable ssTable;
    private final SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> memTable;

    public LsmDao(Path path) throws IOException {
        logger = new Logger(path);
        ssTable = new SSTable(path);
        memTable = createMemTable();
    }

    @Override
    public Iterator<Entry<ComparableMemorySegmentWrapper>> get(ComparableMemorySegmentWrapper from, ComparableMemorySegmentWrapper to) throws IOException {
        final Iterator<Entry<ComparableMemorySegmentWrapper>> sstableIterator = ssTable.get(from, to);

        if (from == null && to == null) {
            return MergeIterator.instanceOf(List.of(sstableIterator, memTable.values().iterator()));
        }

        if (from == null) {
            return MergeIterator.instanceOf(List.of(sstableIterator, memTable.headMap(to).values().iterator()));
        }

        if (to == null) {
            return MergeIterator.instanceOf(List.of(sstableIterator, memTable.tailMap(from).values().iterator()));
        }

        return MergeIterator.instanceOf(List.of(sstableIterator, memTable.subMap(from, to).values().iterator()));
    }

    @Override
    public Entry<ComparableMemorySegmentWrapper> get(ComparableMemorySegmentWrapper key) throws IOException {
        Entry<ComparableMemorySegmentWrapper> comparableMemorySegmentWrapperEntry = memTable.get(key);
        if (comparableMemorySegmentWrapperEntry != null) {
            return comparableMemorySegmentWrapperEntry;
        }

        return ssTable.get(key);
    }

    @Override
    public void upsert(Entry<ComparableMemorySegmentWrapper> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        try {
            memTable.put(entry.key(), entry);
            logger.append(entry.key().getMemorySegment().asByteBuffer(), entry.value().getMemorySegment().asByteBuffer());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    @Override
    public void flush() throws IOException {
        ssTable.flush(memTable.values().iterator());
        logger.clear();
        memTable.clear();
    }

    private SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> createMemTable() throws IOException {
        final Map<ByteBuffer, ByteBuffer> loadedData = logger.read();
        if (loadedData.isEmpty()) {
            return new ConcurrentSkipListMap<>();
        }

        return loadedData.entrySet()
                .stream()
                .map(this::convert)
                .collect(Collectors.toMap(Entry::key, e -> e, (o1, o2) -> o1, ConcurrentSkipListMap::new));
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
