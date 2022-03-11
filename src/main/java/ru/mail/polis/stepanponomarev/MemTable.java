package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable {
    private final AtomicLong sizeBytes;
    private SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> store;

    public MemTable(Iterator<Entry<OSXMemorySegment>> initData) {
        store = new ConcurrentSkipListMap<>();
        sizeBytes = new AtomicLong(0);

        long size = 0;
        while (initData.hasNext()) {
            final Entry<OSXMemorySegment> entry = initData.next();
            store.put(entry.key(), entry);

            size += Utils.sizeOf(entry);
        }

        sizeBytes.set(size);
    }

    private MemTable(SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> store, long sizeBytes) {
        this.sizeBytes = new AtomicLong(sizeBytes);
        this.store = store;
    }

    public Entry<OSXMemorySegment> put(Entry<OSXMemorySegment> entry) {
        final OSXMemorySegment key = entry.key();

        final boolean empty = store.isEmpty();
        Entry<OSXMemorySegment> oldElement = store.get(key);
        if (empty != store.isEmpty()) {
            oldElement = null;
        }

        final Entry<OSXMemorySegment> addedEntry = store.put(key, entry);
        final long addedByteSize = Utils.sizeOf(entry) - (oldElement == null ? 0 : oldElement.value().size());

        sizeBytes.addAndGet(addedByteSize);

        return addedEntry;
    }

    public Iterator<Entry<OSXMemorySegment>> get() {
        return get(null, null);
    }

    public long sizeBytes() {
        return sizeBytes.get();
    }

    public int size() {
        return store.size();
    }

    public MemTable getSnapshotAndClean() {
        MemTable memTable = new MemTable(store, sizeBytes.get());

        store = new ConcurrentSkipListMap<>();
        sizeBytes.set(0);

        return memTable;
    }

    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) {
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
