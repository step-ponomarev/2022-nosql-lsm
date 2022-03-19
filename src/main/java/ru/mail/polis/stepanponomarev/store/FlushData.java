package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Iterator;
import java.util.SortedMap;

final class FlushData {
    private final SortedMap<MemorySegment, TimestampEntry> store;
    public final long sizeBytes;
    public final long timestamp;

    public FlushData(SortedMap<MemorySegment, TimestampEntry> flushData, long sizeBytes) {
        this.store = flushData;
        this.sizeBytes = sizeBytes;
        this.timestamp = System.currentTimeMillis();
    }

    public int getCount() {
        return store.size();
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        return Utils.slice(store, from, to);
    }

    public Iterator<TimestampEntry> get() {
        return store.values().iterator();
    }
}
