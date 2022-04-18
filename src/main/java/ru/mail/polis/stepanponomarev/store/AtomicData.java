package ru.mail.polis.stepanponomarev.store;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AtomicData {
    public final SortedMap<MemorySegment, TimestampEntry> memTable;
    public final SortedMap<MemorySegment, TimestampEntry> flushData;
    public final boolean flushing;

    public AtomicData(SortedMap<MemorySegment, TimestampEntry> memTable,
                      SortedMap<MemorySegment, TimestampEntry> flushData
    ) {
        this(memTable, flushData, false);
    }

    private AtomicData(
            SortedMap<MemorySegment, TimestampEntry> memTable,
            SortedMap<MemorySegment, TimestampEntry> flushData,
            boolean flushing
    ) {
        this.memTable = memTable;
        this.flushData = flushData;
        this.flushing = flushing;
    }

    public static AtomicData beforeFlush(AtomicData data) {
        return new AtomicData(
                new ConcurrentSkipListMap<>(Utils.COMPARATOR),
                new ConcurrentSkipListMap<>(data.memTable),
                true
        );
    }

    public static AtomicData afterFlush(AtomicData data) {
        return new AtomicData(data.memTable, new ConcurrentSkipListMap<>(Utils.COMPARATOR), false);
    }
}
