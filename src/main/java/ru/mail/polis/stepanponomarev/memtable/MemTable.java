package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;

import java.util.Iterator;
import java.util.SortedMap;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, TimestampEntry> store;

    public MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store) {
        this.store = store;
    }

    public Iterator<TimestampEntry> get() {
        return get(null, null);
    }

    public TimestampEntry put(TimestampEntry entry) {
        return store.put(entry.key(), entry);
    }

    public boolean isEmpty() {
        return store.isEmpty();
    }

    public int size() {
        return store.size();
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
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
}
