package ru.mail.polis.stepanponomarev.store;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

public class AtomicData {
    public final SortedMap<MemorySegment, TimestampEntry> memTable;
    public final AtomicLong memTableSizeBytes;

    public final SortedMap<MemorySegment, TimestampEntry> flushData;
    public final AtomicLong flushDataSizeBytes;

    public AtomicData(
        SortedMap<MemorySegment, TimestampEntry> memTable,
        AtomicLong memTableSizeBytes,
        SortedMap<MemorySegment, TimestampEntry> flushData,
        AtomicLong flushDataSizeBytes
    ) {
        this.memTable = memTable;
        this.memTableSizeBytes = memTableSizeBytes;

        this.flushData = flushData;
        this.flushDataSizeBytes = flushDataSizeBytes;
    }

    public static AtomicData beforeFlush(AtomicData data) {
        return new AtomicData(
            new ConcurrentSkipListMap<>(Utils.COMPARATOR),
            new AtomicLong(),
            data.memTable,
            data.memTableSizeBytes
        );
    }

    public static AtomicData afterFlush(AtomicData data) {
        return new AtomicData(data.memTable,
            data.memTableSizeBytes,
            new ConcurrentSkipListMap<>(Utils.COMPARATOR),
            new AtomicLong()
        );
    }
}
