package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, TimestampEntry> store;
    private final FlushData flushData;

    public static final class FlushData {
        private final SortedMap<OSXMemorySegment, TimestampEntry> store;
        public final long sizeBytes;
        public final int count;
        public final long timestamp;

        public FlushData(SortedMap<OSXMemorySegment, TimestampEntry> flushData, long sizeBytes, int count) {
            this.store = flushData;
            this.sizeBytes = sizeBytes;
            this.count = count;
            this.timestamp = System.nanoTime();
        }

        public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
            return slice(store, from, to);
        }

        public Iterator<TimestampEntry> get() {
            return slice(store, null, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FlushData flushData = (FlushData) o;
            return sizeBytes == flushData.sizeBytes && count == flushData.count && timestamp == flushData.timestamp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sizeBytes, count, timestamp);
        }
    }

    public MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store) {
        this.store = store;
        this.flushData = new FlushData(
                Collections.emptyNavigableMap(),
                0,
                0
        );
    }

    private MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store, FlushData flushData) {
        this.store = store;
        this.flushData = flushData;
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

        final FlushData flushData = new FlushData(
                clone,
                sizeBytes,
                clone.size()
        );

        return new MemTable(new ConcurrentSkipListMap<>(), flushData);
    }

    public static MemTable createFlushNullable(MemTable memTableWizard) {
        return new MemTable(memTableWizard.store);
    }

    public Iterator<TimestampEntry> get(
            OSXMemorySegment from,
            OSXMemorySegment to
    ) {
        return slice(store, from, to);
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

    public FlushData getFlushData() {
        return flushData;
    }
}
