package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Iterator;
import java.util.SortedMap;

final class FlushData {
    private final SortedMap<MemorySegment, TimestampEntry> store;
    public final long sizeBytes;
    public final int count;
    public final long timeNs;

    public FlushData(SortedMap<MemorySegment, TimestampEntry> flushData, long sizeBytes, int count) {
        this.store = flushData;
        this.sizeBytes = sizeBytes;
        this.count = count;
        this.timeNs = System.nanoTime();
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        return Utils.slice(store, from, to);
    }

    public Iterator<TimestampEntry> get() {
        return Utils.slice(store, null, null);
    }
}
