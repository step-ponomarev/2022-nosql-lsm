package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

class AtomicStore {
    private final SortedMap<MemorySegment, TimestampEntry> memTable;
    private final SortedMap<MemorySegment, TimestampEntry> flushedTable;
    private final long sizeBytes;

    public AtomicStore(SortedMap<MemorySegment, TimestampEntry> memTable) {
        this.memTable = memTable;
        this.flushedTable = Collections.emptyNavigableMap();
        this.sizeBytes = 0;
    }

    private AtomicStore(SortedMap<MemorySegment, TimestampEntry> memTable,
                       SortedMap<MemorySegment, TimestampEntry> flushedTable
    ) {
        this.memTable = memTable;
        this.flushedTable = flushedTable;
        this.sizeBytes = flushedTable.values()
                .stream()
                .mapToLong(TimestampEntry::getSizeBytes)
                .sum();
    }

    public static AtomicStore prepareToFlush(AtomicStore store) {
        return new AtomicStore(
                new ConcurrentSkipListMap<>(Utils.COMPARATOR),
                new ConcurrentSkipListMap<>(store.memTable)
        );
    }

    public static AtomicStore afterFlush(AtomicStore store) {
        return new AtomicStore(
                store.memTable,
                Collections.emptyNavigableMap()
        );
    }

    public TimestampEntry put(TimestampEntry entry) {
        return memTable.put(entry.key(), entry);
    }

    public SortedMap<MemorySegment, TimestampEntry> getMemTable() {
        return memTable;
    }

    public SortedMap<MemorySegment, TimestampEntry> getFlushedTable() {
        return flushedTable;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }
}
