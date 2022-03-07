package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LmsDao implements Dao<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> {
    private final SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> memTable = new ConcurrentSkipListMap<>();
    private final Log log;

    public LmsDao(Path path) throws IOException {
        log = new Log(path);
    }

    @Override
    public Iterator<Entry<ComparableMemorySegmentWrapper>> get(ComparableMemorySegmentWrapper from, ComparableMemorySegmentWrapper to) {
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
    public void upsert(Entry<ComparableMemorySegmentWrapper> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        memTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {

    }
}
