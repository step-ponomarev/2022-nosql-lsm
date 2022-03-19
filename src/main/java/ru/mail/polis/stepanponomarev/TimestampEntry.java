package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

//TODO: Задел на будущее
public final class TimestampEntry implements Entry<MemorySegment> {
    private final Entry<MemorySegment> entry;
    private final long timestamp;

    public TimestampEntry(Entry<MemorySegment> entry) {
        this.entry = entry;
        this.timestamp = System.currentTimeMillis();
    }

    public TimestampEntry(Entry<MemorySegment> entry, long timestamp) {
        this.entry = entry;
        this.timestamp = timestamp;
    }

    public TimestampEntry(MemorySegment key, MemorySegment value, long timestamp) {
        this.entry = new BaseEntry<>(key, value);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public MemorySegment key() {
        return entry.key();
    }

    @Override
    public MemorySegment value() {
        return entry.value();
    }
}
