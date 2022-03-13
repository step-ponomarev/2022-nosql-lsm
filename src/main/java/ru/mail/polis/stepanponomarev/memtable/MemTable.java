package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class MemTable {
    private static final FlushData EMPTY_FLUSH_DATA = new FlushData(Collections.emptyNavigableMap(),0,0);

    private final SortedMap<OSXMemorySegment, TimestampEntry> store;
    private final List<FlushData> flushSnapshots;
    private final FlushData currentFlushData;

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
        this(store, Collections.emptyList());
    }

    private MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store, List<FlushData> flushSnapshots) {
        this(
                store,
                flushSnapshots,
                EMPTY_FLUSH_DATA
        );
    }

    private MemTable(
            SortedMap<OSXMemorySegment, TimestampEntry> store,
            List<FlushData> flushSnapshots,
            FlushData currentFlushData
    ) {
        this.store = store;
        this.flushSnapshots = flushSnapshots;
        this.currentFlushData = currentFlushData;
    }

    public static MemTable createPreparedToFlush(MemTable memTable) {
        final SortedMap<OSXMemorySegment, TimestampEntry> clone = new ConcurrentSkipListMap<>();

        long sizeBytes = 0;
        final Iterator<TimestampEntry> timestampEntryIterator = slice(memTable.store, null, null);
        while (timestampEntryIterator.hasNext()) {
            TimestampEntry entry = timestampEntryIterator.next();

            sizeBytes += Utils.sizeOf(entry);
            clone.put(entry.key(), entry);
        }

        final FlushData currentFlushData = clone.isEmpty()
                ? EMPTY_FLUSH_DATA
                : new FlushData(clone, sizeBytes, clone.size());

        final List<FlushData> flushSnapshot = new CopyOnWriteArrayList<>(memTable.flushSnapshots);
        if (!currentFlushData.equals(EMPTY_FLUSH_DATA)) {
            flushSnapshot.add(currentFlushData);
        }

        return new MemTable(new ConcurrentSkipListMap<>(), Collections.unmodifiableList(flushSnapshot), currentFlushData);
    }

    public static MemTable createFlushNullable(MemTable memTableWizard) {
        final CopyOnWriteArrayList<FlushData> flushSnapshot = new CopyOnWriteArrayList<>(memTableWizard.flushSnapshots);
        flushSnapshot.remove(memTableWizard.currentFlushData);

        return new MemTable(memTableWizard.store, Collections.unmodifiableList(flushSnapshot));
    }

    public Iterator<TimestampEntry> get(
            OSXMemorySegment from,
            OSXMemorySegment to
    ) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(flushSnapshots.size() + 1);
//        for (FlushData flushData : flushSnapshots) {
//            data.add(flushData.get(from, to));
//        }

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

    public FlushData getCurrentFlushData() {
        return currentFlushData;
    }
}
