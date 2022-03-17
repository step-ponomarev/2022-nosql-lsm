package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

final class AtomicStore implements Closeable {
    private final List<SSTable> ssTables;
    private final Map<Long, FlushData> flushData;
    private final SortedMap<MemorySegment, TimestampEntry> memTable;

    public AtomicStore(List<SSTable> ssTables, SortedMap<MemorySegment, TimestampEntry> memTable) {
        this(ssTables, Collections.emptyMap(), memTable);
    }

    private AtomicStore(
            List<SSTable> ssTables,
            Map<Long, FlushData> flushData,
            SortedMap<MemorySegment, TimestampEntry> memTable
    ) {
        this.ssTables = ssTables;
        this.flushData = flushData;
        this.memTable = memTable;
    }

    //TODO: Слишком много аллокаций
    public static AtomicStore prepareToFlush(AtomicStore flushStore, long timestamp) {
        if (flushStore.flushData.containsKey(timestamp)) {
            throw new IllegalStateException("Trying to flush already flushed data.");
        }

        if (flushStore.memTable.isEmpty()) {
            return new AtomicStore(
                    new ArrayList<>(flushStore.ssTables),
                    new ConcurrentSkipListMap<>(flushStore.memTable)
            );
        }

        long size = 0;
        final SortedMap<MemorySegment, TimestampEntry> flushingData = Utils.createMap();
        for (TimestampEntry entry : flushStore.memTable.values()) {
            size += Utils.sizeOf(entry);
            flushingData.put(entry.key(), entry);
        }

        final FlushData flushData = new FlushData(flushingData, size, flushingData.size());
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushData);
        flushSnapshots.put(timestamp, flushData);

        return new AtomicStore(
                flushStore.ssTables,
                flushSnapshots,
                filterByTimestamp(flushStore.memTable, timestamp)
        );
    }

    private static SortedMap<MemorySegment, TimestampEntry> filterByTimestamp(
            SortedMap<MemorySegment, TimestampEntry> source,
            long timestamp
    ) {
        return source.entrySet()
                .stream()
                .filter(e -> e.getValue().getTimestamp() > timestamp)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (o1, o2) -> o1,
                        Utils::createMap)
                );
    }

    public static AtomicStore afterFlush(AtomicStore flushStore, SSTable newSSTable, long timestamp) {
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushData);
        flushSnapshots.remove(timestamp);

        List<SSTable> newSSTables = new ArrayList<>(flushStore.ssTables);
        newSSTables.add(newSSTable);

        return new AtomicStore(newSSTables, flushSnapshots, Utils.createMap());
    }

    @Override
    public void close() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public SortedMap<MemorySegment, TimestampEntry> getMemTable() {
        return memTable;
    }

    public FlushData getFlushData(long timestamp) {
        return flushData.get(timestamp);
    }

    public TimestampEntry get(MemorySegment key) {
        final TimestampEntry memoryEntry = this.memTable.get(key);
        if (memoryEntry != null) {
            return memoryEntry;
        }

        final Iterator<TimestampEntry> data = get(key, null);
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            if (Utils.compare(entry.key(), key) == 0) {
                return entry;
            }
        }

        return null;
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(ssTables.size() + flushData.size() + 1);
        for (SSTable ssTable : ssTables) {
            data.add(ssTable.get(from, to));
        }

        for (FlushData fd : flushData.values()) {
            data.add(fd.get(from, to));
        }

        data.add(Utils.slice(memTable, from, to));

        return Utils.merge(data);
    }
}
