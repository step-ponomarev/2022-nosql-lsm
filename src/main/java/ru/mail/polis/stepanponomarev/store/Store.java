package ru.mail.polis.stepanponomarev.store;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class Store {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final AtomicLong sizeBytes;
    private volatile AtomicStore atomicStore;

    public Store(Path path, Iterator<TimestampEntry> initData) throws IOException {
        this.path = path;

        final SortedMap<OSXMemorySegment, TimestampEntry> memTable = new ConcurrentSkipListMap<>();
        long initSizeBytes = 0;
        while (initData.hasNext()) {
            final TimestampEntry entry = initData.next();
            initSizeBytes += Utils.sizeOf(entry);
            memTable.put(entry.key(), entry);
        }

        this.sizeBytes = new AtomicLong(initSizeBytes);
        this.atomicStore = new AtomicStore(wakeUpSSTables(path), memTable);
    }

    public void flush(long timestamp) throws IOException {
        final long sizeBytesBeforeFlush = sizeBytes.get();

        atomicStore = AtomicStore.prepareToFlush(atomicStore, timestamp);
        final FlushData flushData = atomicStore.getFlushData(timestamp);
        if (flushData == null) {
            return;
        }

        final Path sstablePath = path.resolve(SSTABLE_DIR_NAME + timestamp);
        Files.createDirectory(sstablePath);

        SSTable newSSTable = SSTable.createInstance(sstablePath, flushData.get(), flushData.sizeBytes, flushData.count);
        atomicStore = AtomicStore.afterFlush(atomicStore, newSSTable, timestamp);

        sizeBytes.addAndGet(-sizeBytesBeforeFlush);
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        return atomicStore.get(from, to);
    }

    public void put(TimestampEntry entry) {
        atomicStore.getMemTable().put(entry.key(), entry);
        sizeBytes.addAndGet(Utils.sizeOf(entry));
    }

    public long getSizeBytes() {
        return sizeBytes.get();
    }

    private CopyOnWriteArrayList<SSTable> wakeUpSSTables(Path path) throws IOException {
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
