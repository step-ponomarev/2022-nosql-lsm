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

public class LsmDao implements Dao<OSXMemorySegment, Entry<OSXMemorySegment>> {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final List<SSTable> store;
    private final SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> memTable = new ConcurrentSkipListMap<>();

    private volatile long memTableSize;

    public LsmDao(Path bathPath) throws IOException {
        path = bathPath;
        store = createStore(path);
        memTableSize = 0;
    }

    @Override
    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<Entry<OSXMemorySegment>>> iterators = new ArrayList<>();
        for (SSTable table : store) {
            iterators.add(table.get(from, to));
        }

        final Iterator<Entry<OSXMemorySegment>> memTableIterator = getMemTableIterator(from, to);
        iterators.add(memTableIterator);

        return MergedIterator.instanceOf(iterators);
    }

    private Iterator<Entry<OSXMemorySegment>> getMemTableIterator(OSXMemorySegment from, OSXMemorySegment to) {
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
    public synchronized void upsert(Entry<OSXMemorySegment> entry) {
        memTable.put(entry.key(), entry);
        memTableSize += entry.key().getMemorySegment().byteSize() + Long.BYTES * 2;

        final OSXMemorySegment value = entry.value();
        if (value != null) {
            memTableSize += value.getMemorySegment().byteSize();
        }
    }

    @Override
    public void flush() throws IOException {
        final Path dir = path.resolve(SSTABLE_DIR_NAME + store.size());
        if (Files.notExists(dir)) {
            Files.createDirectory(dir);
        }

        store.add(SSTable.createInstance(dir, memTable.values().iterator(), memTableSize));
        synchronized (this) {
            memTable.clear();
            memTableSize = 0;
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
