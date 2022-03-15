package ru.mail.polis.stepanponomarev.store;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Store {
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
        this.atomicStore = new AtomicStore(SSTable.wakeUpSSTables(path), memTable);
    }

    public void flush(long timestamp) throws IOException {
        final long sizeBytesBeforeFlush = sizeBytes.get();

        atomicStore = AtomicStore.prepareToFlush(atomicStore, timestamp);
        final FlushData flushData = atomicStore.getFlushData(timestamp);
        if (flushData == null) {
            return;
        }

        SSTable newSSTable = SSTable.createInstance(
                path,
                flushData.get(),
                flushData.sizeBytes,
                flushData.count,
                timestamp
        );
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
}
