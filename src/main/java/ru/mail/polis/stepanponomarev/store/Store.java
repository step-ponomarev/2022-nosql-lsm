package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class Store implements Closeable {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final AtomicLong sizeBytes;
    
    private final List<SSTable> ssTables;
    private final SortedMap<MemorySegment, TimestampEntry> memTable;

    public Store(Path path, Iterator<TimestampEntry> data) throws IOException {
        this.path = path;
        
        this.ssTables = wakeUpSSTables(path);
        this.memTable = new ConcurrentSkipListMap<>(Utils.COMPARATOR);
        long initSyzeBytes = 0;
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            initSyzeBytes += entry.getSizeBytes();
            this.memTable.put(entry.key(), entry);
        }
        this.sizeBytes = new AtomicLong(initSyzeBytes);
    }

    @Override
    public void close() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public void flush(long timestamp) throws IOException {
        final long sizeBytesBeforeFlush = sizeBytes.get();
        final Collection<TimestampEntry> values = memTable.values();

        final Path sstableDir = path.resolve(SSTABLE_DIR_NAME + timestamp + System.nanoTime());
        Files.createDirectory(sstableDir);

        final SSTable ssTable = SSTable.createInstance(
                sstableDir,
                values.iterator(),
                sizeBytes.get(),
                values.size()
        );
        
        ssTables.add(ssTable);
        
        memTable.clear();
        sizeBytes.addAndGet(-sizeBytesBeforeFlush);
    }

    public TimestampEntry get(MemorySegment key) {
        final TimestampEntry memoryEntry = this.memTable.get(key);
        if (memoryEntry != null) {
            return memoryEntry;
        }

        final Iterator<TimestampEntry> data = get(key, null);
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            if (Utils.compare(key, entry.key()) == 0) {
                return entry;
            }
        }

        return null;
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(ssTables.size() + 2);
        for (SSTable ssTable : ssTables) {
            data.add(ssTable.get(from, to));
        }

        data.add(slice(memTable, from, to));

        return MergedIterator.of(data, Utils.COMPARATOR);
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
        memTable.put(entry.key(), entry);
        sizeBytes.addAndGet(entry.getSizeBytes());
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
