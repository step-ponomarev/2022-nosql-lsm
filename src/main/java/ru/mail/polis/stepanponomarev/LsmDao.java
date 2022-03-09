package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LsmDao implements Dao<OSXMemorySegment, Entry<OSXMemorySegment>> {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final List<SSTable> store;
    private final SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> memTable = new ConcurrentSkipListMap<>();

    private final Object sizeCounterLock = new Object();
    private volatile long memTableSize = 0;

    public LsmDao(Path bathPath) throws IOException {
        path = bathPath;
        store = createStore(path);
    }

    @Override
    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<Entry<OSXMemorySegment>>> iterators = new ArrayList<>();
        for (SSTable table : store) {
            iterators.add(table.get(from, to));
        }

        if (from == null && to == null) {
            iterators.add(memTable.values().iterator());
        } else if (from == null) {
            iterators.add(memTable.headMap(to).values().iterator());
        } else if (to == null) {
            iterators.add(memTable.tailMap(from).values().iterator());
        } else {
            iterators.add(memTable.subMap(from, to).values().iterator());
        }

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(Entry<OSXMemorySegment> entry) {
        synchronized (sizeCounterLock) {
            memTable.put(entry.key(), entry);
            memTableSize += entry.key().getMemorySegment().byteSize() + Long.BYTES * 2;

            final OSXMemorySegment value = entry.value();
            if (value != null) {
                memTableSize += value.getMemorySegment().byteSize();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        final Path dir = path.resolve(SSTABLE_DIR_NAME + store.size());
        if (Files.notExists(dir)) {
            Files.createDirectory(dir);
        }

        final Future<SSTable> sstableCreation = Executors.newSingleThreadExecutor().
                submit(() -> SSTable.createInstance(dir, memTable.values().iterator(), memTableSize));
        try {
            store.add(sstableCreation.get());
            synchronized (sizeCounterLock) {
                memTable.clear();
                memTableSize = 0;
            }
        } catch (Exception e) {
            throw new IOException("Something wrong with flush.");
        }
    }

    private List<SSTable> createStore(Path path) throws IOException {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        final String[] dirList = path.toFile().list();
        final int ssTableAmount = dirList == null ? 0 : dirList.length;
        final List<SSTable> tables = new ArrayList<>(ssTableAmount);
        for (int i = 0; i < ssTableAmount; i++) {
            tables.add(SSTable.upInstance(path.resolve(SSTABLE_DIR_NAME + i)));
        }

        return tables;
    }
}
