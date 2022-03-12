package ru.mail.polis.stepanponomarev;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class MemTable {
    private final AtomicLong sizeBytes;
    private SortedMap<OSXMemorySegment, EntryWithTime> store;

    private final Lock putLock;
    private final Lock clearLock;

    public MemTable(Iterator<EntryWithTime> initData) {
        store = new ConcurrentSkipListMap<>();
        sizeBytes = new AtomicLong(0);

        long size = 0;
        while (initData.hasNext()) {
            final EntryWithTime entry = initData.next();
            store.put(entry.key(), entry);

            size += Utils.sizeOf(entry);
        }

        sizeBytes.set(size);

        final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        putLock = readWriteLock.readLock();
        clearLock = readWriteLock.writeLock();
    }

    private MemTable(SortedMap<OSXMemorySegment, EntryWithTime> store, long sizeBytes) {
        this.sizeBytes = new AtomicLong(sizeBytes);
        this.store = store;

        final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        putLock = readWriteLock.readLock();
        clearLock = readWriteLock.writeLock();
    }

    // Тут все правильно считает в многопоточной среде
    public EntryWithTime put(EntryWithTime entry) {
        try {
            putLock.lock();

            final OSXMemorySegment key = entry.key();
            final EntryWithTime oldElement = store.get(key);

            final EntryWithTime addedEntry = store.put(key, entry);
            final long oldValueSizeBytes = (oldElement == null ? 0
                    : oldElement.getTimestamp() <= entry.getTimestamp()
                    ? oldElement.value().size()
                    : 0
            );

            final long addedByteSize = Utils.sizeOf(entry) - oldValueSizeBytes;
            long size;
            do {
                size = sizeBytes.get();
            } while (!sizeBytes.compareAndSet(size, addedByteSize + size));

            return addedEntry;
        } finally {
            putLock.unlock();
        }
    }

    public Iterator<EntryWithTime> get() {
        return get(null, null);
    }

    public MemTable getSnapshotAndClear() {
        try {
            clearLock.lock();

            long sizeBefore = sizeBytes.get();
            final MemTable memTable = new MemTable(store, sizeBefore);

            store = new ConcurrentSkipListMap<>();

            long size;
            do {
                size = sizeBytes.get();
            } while (!sizeBytes.compareAndSet(size, size - sizeBefore));

            return memTable;
        } finally {
            clearLock.unlock();
        }
    }

    public long sizeBytes() {
        return sizeBytes.get();
    }

    public int size() {
        return store.size();
    }

    public Iterator<EntryWithTime> get(OSXMemorySegment from, OSXMemorySegment to) {
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
