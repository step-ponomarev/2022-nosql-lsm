package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class Store implements Closeable {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    
    private final CopyOnWriteArrayList<SSTable> ssTables;
    private final AtomicReference<AtomicStore> atomicStore;

    public Store(Path path) throws IOException {
        this.path = path;
        this.ssTables = wakeUpSSTables(path);
        this.atomicStore = new AtomicReference<>(
                new AtomicStore(new ConcurrentSkipListMap<>(Utils.COMPARATOR))
        );
    }

    @Override
    public void close() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public void flush(long timestamp) throws IOException {
        atomicStore.set(AtomicStore.prepareToFlush(atomicStore.get()));
        final AtomicStore flushedStore = this.atomicStore.get();
        if (flushedStore.getFlushedTable().isEmpty()) {
            return;
        }

        final Path sstableDir = path.resolve(SSTABLE_DIR_NAME + timestamp + System.nanoTime());
        Files.createDirectory(sstableDir);
        final SSTable ssTable = SSTable.createInstance(
                sstableDir,
                flushedStore.getFlushedTable().values().iterator(),
                flushedStore.getSizeBytes(),
                flushedStore.getFlushedTable().size()
        );

        ssTables.add(ssTable);
        atomicStore.set(AtomicStore.afterFlush(atomicStore.get()));
    }

    public TimestampEntry get(MemorySegment key) {
        final TimestampEntry memoryEntry = atomicStore.get().getMemTable().get(key);
        if (memoryEntry != null) {
            return memoryEntry.value() == null ? null : memoryEntry;
        }

        final Iterator<TimestampEntry> data = get(key, null);
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            if (Utils.compare(key, entry.key()) == 0) {
                return entry.value() == null ? null : entry;
            }
        }

        return null;
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final List<Iterator<TimestampEntry>> entries = new ArrayList<>(ssTables.size() + 2);
        for (SSTable ssTable : ssTables) {
            entries.add(ssTable.get(from, to));
        }

        entries.add(slice(atomicStore.get().getFlushedTable(), from, to));
        entries.add(slice(atomicStore.get().getMemTable(), from, to));

        return MergeIterator.of(entries, Utils.COMPARATOR);
    }
    
    private static Iterator<TimestampEntry> slice(
            SortedMap<MemorySegment, TimestampEntry> store,
            MemorySegment from,
            MemorySegment to
    ) {
        if (store == null || store.isEmpty()) {
            return Collections.emptyIterator();
        }

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

    public void put(TimestampEntry entry) {
        atomicStore.get().put(entry);
    }

    private static CopyOnWriteArrayList<SSTable> wakeUpSSTables(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            final List<String> tableDirNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .sorted()
                    .toList();

            final CopyOnWriteArrayList<SSTable> tables = new CopyOnWriteArrayList<>();
            for (String name : tableDirNames) {
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }
}
