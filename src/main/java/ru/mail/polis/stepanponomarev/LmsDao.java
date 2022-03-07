package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LmsDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final SortedMap<MemorySegment, Entry<MemorySegment>> memTable = new ConcurrentSkipListMap<>(Utils.MEMORY_SEGMENT_COMPARATOR);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
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

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        memTable.put(entry.key(), entry);
    }
}
