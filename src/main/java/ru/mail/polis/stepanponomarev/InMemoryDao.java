package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final static Comparator<MemorySegment> memorySegmentComparator = (MemorySegment o1, MemorySegment o2) -> {
        if (o1 == o2) {
            return 0;
        }

        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }

        byte byteAtOffset1 = MemoryAccess.getByteAtOffset(o1, mismatch);
        byte byteAtOffset2 = MemoryAccess.getByteAtOffset(o2, mismatch);

        return byteAtOffset1 < byteAtOffset2 ? -1 : 1;
    };

    private final SortedMap<MemorySegment, Entry<MemorySegment>> store = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
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

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        store.put(entry.key(), entry);
    }
}
