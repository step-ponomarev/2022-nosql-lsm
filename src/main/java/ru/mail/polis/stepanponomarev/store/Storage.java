package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.TombstoneSkipIterator;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public final class Storage implements Closeable {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final CopyOnWriteArrayList<SSTable> ssTables;
    private final SortedMap<MemorySegment, TimestampEntry> memTable;

    public Storage(Path path) throws IOException {
        this.path = path;
        this.ssTables = wakeUpSSTables(path);
        this.memTable = new ConcurrentSkipListMap<>(Utils.COMPARATOR);
    }

    @Override
    public void close() {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public void flush(long timestamp) throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        ssTables.add(
                flush(memTable, timestamp)
        );
    }

    public void compact(long timestamp) throws IOException {
        final Iterator<TimestampEntry> dataIterator = new TombstoneSkipIterator<>(get(null, null));
        if (!dataIterator.hasNext()) {
            return;
        }

        final SortedMap<MemorySegment, TimestampEntry> data = new ConcurrentSkipListMap<>(Utils.COMPARATOR);
        while (dataIterator.hasNext()) {
            TimestampEntry entry = dataIterator.next();
            data.put(entry.key(), entry);
        }

        //TODO: ОПАСНОСТЬ!!11
        final SSTable flushedSSTable = flush(data, timestamp);
        ssTables.clear();
        ssTables.add(flushedSSTable);

        removeFilesWithNaster(
                getSSTablesOlderThan(path, timestamp)
        );
    }

    private static List<Path> getSSTablesOlderThan(Path path, long timestamp) throws IOException {
        try (Stream<Path> files = Files.walk(path)) {
            return files
                    .filter(f -> f.getFileName().toString().contains(SSTABLE_DIR_NAME))
                    .filter(f -> !f.getFileName().toString().contains(String.valueOf(timestamp)))
                    .toList();
        }
    }

    private static void removeFilesWithNaster(List<Path> files) throws IOException {
        for (Path dirs : files) {
            try (Stream<Path> ssTableFiles = Files.walk(dirs)) {
                final Iterator<Path> filesToRemove = ssTableFiles.sorted(Comparator.reverseOrder()).iterator();
                while (filesToRemove.hasNext()) {
                    Files.delete(filesToRemove.next());
                }
            }
        }
    }

    private SSTable flush(SortedMap<MemorySegment, TimestampEntry> data, long timestamp) throws IOException {
        final long sizeBytes = data.values()
                .stream()
                .mapToLong(TimestampEntry::getSizeBytes)
                .sum();

        final Path sstableDir = path.resolve(SSTABLE_DIR_NAME + getHash(timestamp));
        Files.createDirectory(sstableDir);

        return SSTable.createInstance(
                sstableDir,
                data.values().iterator(),
                sizeBytes,
                data.size()
        );
    }

    private static String getHash(long timestamp) {
        final int HASH_SIZE = 30;

        StringBuilder hash = new StringBuilder(timestamp + String.valueOf(System.nanoTime()));
        while (hash.length() < HASH_SIZE) {
            hash.append(0);
        }

        return hash.substring(0, HASH_SIZE);
    }

    public TimestampEntry get(MemorySegment key) {
        final TimestampEntry memoryEntry = memTable.get(key);
        if (memoryEntry != null) {
            return memoryEntry.value() == null ? null : memoryEntry;
        }

        final Iterator<TimestampEntry> data = get(key, null);
        if (!data.hasNext()) {
            return null;
        }

        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            if (Utils.compare(key, entry.key()) == 0) {
                return entry.value() == null ? null : entry;
            }

            if (Utils.compare(key, entry.key()) < 0) {
                return null;
            }
        }

        return null;
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final List<Iterator<TimestampEntry>> entries = new ArrayList<>(ssTables.size() + 2);
        for (SSTable ssTable : ssTables) {
            entries.add(ssTable.get(from, to));
        }

        entries.add(slice(memTable, from, to));

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
        memTable.put(entry.key(), entry);
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
