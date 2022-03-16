package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.util.Objects;

public final class TimestampEntry implements Entry<MemorySegment> {
    private final Entry<MemorySegment> entry;
    private final long timestamp;

    public TimestampEntry(Entry<MemorySegment> entry) {
        this.entry = entry;
        this.timestamp = System.nanoTime();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass() || this.hashCode() != o.hashCode()) {
            return false;
        }

        TimestampEntry entry1 = (TimestampEntry) o;
        return timestamp == entry1.timestamp && Objects.equals(entry, entry1.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, timestamp);
    }
}
