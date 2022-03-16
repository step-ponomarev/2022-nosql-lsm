package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> memTable = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeBytes = new AtomicLong(0);

    public Entry<OSXMemorySegment> put(OSXMemorySegment key, Entry<OSXMemorySegment> value) {
        final Entry<OSXMemorySegment> oldElement = memTable.get(key);

        synchronized (this) {
            final Entry<OSXMemorySegment> entry = memTable.put(key, value);

            final long addedByteSize = key.size()
                    - (oldElement == null ? 0 : oldElement.value().size())
                    + (value == null ? 0 : value.value().size());

            sizeBytes.addAndGet(addedByteSize);

            return entry;
        }
    }

    public Iterator<Entry<OSXMemorySegment>> get() {
        return get(null, null);
    }

    public long sizeBytes() {
        return sizeBytes.get();
    }

    public int size() {
        return memTable.size();
    }

    public synchronized void clear() {
        memTable.clear();
        sizeBytes.set(0);
    }

    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        }

        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }

        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }

        return memTable.subMap(from, to).values().iterator();
    }
}
