package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class Store implements Closeable {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final AtomicLong sizeBytes;
    private final AtomicReference<AtomicStore> atomicStore;

    public Store(Path path, Iterator<TimestampEntry> data) throws IOException {
        this.path = path;

        final SortedMap<MemorySegment, TimestampEntry> memTable = Utils.createMap();
        long initSizeBytes = 0;
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            initSizeBytes += Utils.sizeOf(entry);
            memTable.put(entry.key(), entry);
        }

        this.sizeBytes = new AtomicLong(initSizeBytes);
        this.atomicStore = new AtomicReference<>(new AtomicStore(wakeUpSSTables(path), memTable));
    }

    @Override
    public void close() throws IOException {
        atomicStore.get().close();
    }

    public void flush(long timestamp) throws IOException {
        final long sizeBytesBeforeFlush = sizeBytes.get();

        atomicStore.set(AtomicStore.prepareToFlush(atomicStore.get(), timestamp));
        final FlushData flushData = atomicStore.get().getFlushData(timestamp);
        if (flushData == null) {
            return;
        }

        final Path sstablePath = path.resolve(SSTABLE_DIR_NAME + timestamp);
        Files.createDirectory(sstablePath);

        final SSTable newSSTable = SSTable.createInstance(
                sstablePath,
                flushData.get(),
                flushData.sizeBytes,
                flushData.getCount()
        );
        atomicStore.set(AtomicStore.afterFlush(atomicStore.get(), newSSTable, timestamp));

        sizeBytes.addAndGet(-sizeBytesBeforeFlush);
    }

    public TimestampEntry get(MemorySegment key) {
        return atomicStore.get().get(key);
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        return atomicStore.get().get(from, to);
    }

    public void put(TimestampEntry entry) {
        atomicStore.get().getMemTable().put(entry.key(), entry);
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
