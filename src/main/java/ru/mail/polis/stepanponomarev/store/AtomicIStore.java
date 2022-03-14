package ru.mail.polis.stepanponomarev.store;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

final class AtomicIStore {
    private final List<SSTable> ssTables;
    private final Map<Long, FlushData> flushSnapshots;
    private final SortedMap<OSXMemorySegment, TimestampEntry> memTable;

    public AtomicIStore(List<SSTable> ssTables, SortedMap<OSXMemorySegment, TimestampEntry> memTable) {
        this(ssTables, Collections.emptyMap(), memTable);
    }

    private AtomicIStore(
            List<SSTable> ssTables,
            Map<Long, FlushData> flushSnapshots,
            SortedMap<OSXMemorySegment, TimestampEntry> memTable
    ) {
        this.ssTables = ssTables;
        this.flushSnapshots = flushSnapshots;
        this.memTable = memTable;
    }

    public static AtomicIStore prepareToFlush(AtomicIStore flushStore, long timestamp) {
        if (flushStore.flushSnapshots.containsKey(timestamp)) {
            throw new IllegalStateException("Trying to flush already flushed data.");
        }

        if (flushStore.memTable.isEmpty()) {
            return flushStore;
        }

        long size = 0;
        final ConcurrentSkipListMap<OSXMemorySegment, TimestampEntry> flushingMemTable
                = new ConcurrentSkipListMap<>();

        Iterator<TimestampEntry> iterator = flushStore.memTable.values().iterator();
        while (iterator.hasNext()) {
            final TimestampEntry entry = iterator.next();
            size += Utils.sizeOf(entry);
            flushingMemTable.put(entry.key(), entry);
        }

        final FlushData flushData = new FlushData(flushingMemTable, size, flushingMemTable.size());
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushSnapshots);
        flushSnapshots.put(timestamp, flushData);

        return new AtomicIStore(flushStore.ssTables, flushSnapshots, new ConcurrentSkipListMap<>());
    }

    public static AtomicIStore afterFlush(AtomicIStore flushStore, SSTable newSSTable, long timestamp) {
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushSnapshots);
        flushSnapshots.remove(timestamp);

        List<SSTable> newSSTables = new ArrayList<>(flushStore.ssTables);
        newSSTables.add(newSSTable);

        return new AtomicIStore(newSSTables, flushSnapshots, new ConcurrentSkipListMap<>());
    }

    public SortedMap<OSXMemorySegment, TimestampEntry> getMemTable() {
        return memTable;
    }

    public FlushData getFlushData(long timestamp) {
        return flushSnapshots.get(timestamp);
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(ssTables.size() + flushSnapshots.size() + 1);
        for (SSTable ssTable : ssTables) {
            data.add(ssTable.get(from, to));
        }

        for (FlushData flushData : flushSnapshots.values()) {
            data.add(flushData.get(from, to));
        }

        data.add(Utils.slice(memTable, from, to));

        return MergedIterator.instanceOf(data);
    }
}
