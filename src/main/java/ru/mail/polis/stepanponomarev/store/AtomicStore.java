package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

final class AtomicStore implements Closeable {
    private static final FlushData EMPTY_FLUSH_DATA = new FlushData(Utils.createMap(), 0);

    private final List<SSTable> ssTables;
    private final SortedMap<MemorySegment, TimestampEntry> memTable;
    private final FlushData flushData;

    public AtomicStore(
            List<SSTable> ssTables,
            SortedMap<MemorySegment, TimestampEntry> memTable) {
        this.ssTables = ssTables;
        this.memTable = memTable;
        this.flushData = EMPTY_FLUSH_DATA;
    }
    
    private AtomicStore(
            List<SSTable> ssTables,
            SortedMap<MemorySegment, TimestampEntry> memTable,
            SortedMap<MemorySegment, TimestampEntry> flushData,
            long flushDataSizeBytes
    ) {
        this.ssTables = ssTables;
        this.memTable = memTable;
        this.flushData = new FlushData(flushData, flushDataSizeBytes);
    }

    public static AtomicStore prepareToFlush(AtomicStore flushStore, AtomicLong sizeBytes) {
        if (flushStore.memTable.isEmpty()) {
            return flushStore;
        }

        return new AtomicStore(
                flushStore.ssTables,
                Utils.createMap(),
                new ConcurrentSkipListMap<>(flushStore.memTable),
                sizeBytes.get()
        );
    }

    public static AtomicStore afterFlush(AtomicStore flushStore, SSTable newSSTable) {
        final List<SSTable> newSSTables = new ArrayList<>(flushStore.ssTables);
        newSSTables.add(newSSTable);

        return new AtomicStore(newSSTables, flushStore.memTable, Utils.createMap(), 0);
    }

    @Override
    public void close() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public SortedMap<MemorySegment, TimestampEntry> getMemTable() {
        return memTable;
    }

    public FlushData getFlushData() {
        return flushData;
    }

    public TimestampEntry get(MemorySegment key) {
        final TimestampEntry memoryEntry = this.memTable.get(key);
        if (memoryEntry != null) {
            return memoryEntry;
        }

        final Iterator<TimestampEntry> data = get(key, null);
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            if (Utils.compare(entry.key(), key) == 0) {
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

        data.add(flushData.get(from, to));
        data.add(Utils.slice(memTable, from, to));

        return Utils.merge(data);
    }
}
