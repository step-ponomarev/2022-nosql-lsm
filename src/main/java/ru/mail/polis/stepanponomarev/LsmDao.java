package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
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

//TODO: Добавить лог, дублирующий memTable
public class LsmDao implements Dao<OSXMemorySegment, Entry<OSXMemorySegment>> {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final List<SSTable> store;
    private final SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> memTable = new ConcurrentSkipListMap<>();

    public LsmDao(Path bathPath) throws IOException {
        path = bathPath;
        store = createStore(path);
    }

    @Override
    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        List<Iterator<Entry<OSXMemorySegment>>> iterators = new ArrayList<>();
//        for (SSTable table : store) {
//            iterators.add(table.get(from, to));
//        }

        if (from == null && to == null) {
//            iterators.add(memTable.values().iterator());
            return memTable.values().iterator();

        } else if (from == null) {
            return memTable.headMap(to).values().iterator();
//            iterators.add(memTable.headMap(to).values().iterator());
        } else if (to == null) {
            return memTable.tailMap(from).values().iterator();
//            iterators.add(memTable.tailMap(from).values().iterator());
        } else {
            iterators.add(memTable.subMap(from, to).values().iterator());
            return memTable.subMap(from, to).values().iterator();
        }

//        return MergedIterator.instanceOf(iterators, comparator);
    }

    @Override
    public void upsert(Entry<OSXMemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        final Path dir = path.resolve(SSTABLE_DIR_NAME + store.size());
        if (Files.notExists(dir)) {
            Files.createDirectory(dir);
        }

//        store.add(SSTable.createInstance(dir, memTable.values().iterator()));
        memTable.clear();
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
