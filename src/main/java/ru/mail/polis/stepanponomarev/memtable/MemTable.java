package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, TimestampEntry> store;
    private final Map<Long, FlushData> flushSnapshots;

    public static final class FlushData {
        private final SortedMap<OSXMemorySegment, TimestampEntry> store;
        public final long sizeBytes;
        public final int count;
        public final long timeNs;

        public FlushData(SortedMap<OSXMemorySegment, TimestampEntry> flushData, long sizeBytes, int count) {
            this.store = flushData;
            this.sizeBytes = sizeBytes;
            this.count = count;
            this.timeNs = System.nanoTime();
        }

        public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
            return slice(store, from, to);
        }

        public Iterator<TimestampEntry> get() {
            return slice(store, null, null);
        }
    }

    public MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store) {
        this(store, Collections.emptyMap());
    }

    private MemTable(
            SortedMap<OSXMemorySegment, TimestampEntry> store,
            Map<Long, FlushData> flushSnapshots
    ) {
        this.store = store;
        this.flushSnapshots = flushSnapshots;
    }

    public static MemTable createPreparedToFlush(MemTable memTable, long timestamp) {
        if (memTable.store.isEmpty()) {
            return memTable;
        }

        final SortedMap<OSXMemorySegment, TimestampEntry> storeClone = new ConcurrentSkipListMap<>();

        long sizeBytes = 0;
        final Iterator<TimestampEntry> timestampEntryIterator = slice(memTable.store, null, null);
        while (timestampEntryIterator.hasNext()) {
            TimestampEntry entry = timestampEntryIterator.next();

            sizeBytes += Utils.sizeOf(entry);
            storeClone.put(entry.key(), entry);
        }

        final FlushData currentFlushData = new FlushData(storeClone, sizeBytes, storeClone.size());
        final Map<Long, FlushData> flushSnapshot = new HashMap<>(memTable.flushSnapshots);
        flushSnapshot.put(timestamp, currentFlushData);

        return new MemTable(new ConcurrentSkipListMap<>(), Collections.unmodifiableMap(flushSnapshot));
    }

    public static MemTable createFlushNullable(MemTable memTableWizard, long timestamp) {
        final Map<Long, FlushData> flushSnapshot = new HashMap<>(memTableWizard.flushSnapshots);
        flushSnapshot.remove(timestamp);

        return new MemTable(memTableWizard.store, Collections.unmodifiableMap(flushSnapshot));
    }

    public Iterator<TimestampEntry> get(
            OSXMemorySegment from,
            OSXMemorySegment to
    ) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(flushSnapshots.size() + 1);
        for (FlushData flushData : flushSnapshots.values()) {
            data.add(flushData.get(from, to));
        }

        data.add(slice(store, from, to));

        return MergedIterator.instanceOf(data);
    }

    private static Iterator<TimestampEntry> slice(
            SortedMap<OSXMemorySegment, TimestampEntry> store,
            OSXMemorySegment from,
            OSXMemorySegment to
    ) {
        if (store == null || store.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return store.values().iterator();
        }

        if (from == null) {
            return store.headMap(to).values().iterator();
        }

        if (to == null) {
            return store.tailMap(from).values().iterator();
        }

        return store.subMap(from, to).values().iterator();
    }

    public TimestampEntry put(TimestampEntry entry) {
        return store.put(entry.key(), entry);
    }

    public FlushData getFlushData(long timestamp) {
        return flushSnapshots.get(timestamp);
    }
}
