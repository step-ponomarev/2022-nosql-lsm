package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.util.Objects;

public final class TimestampEntry implements Entry<OSXMemorySegment> {
    private final Entry<OSXMemorySegment> entry;
    private final long timestamp;

    public TimestampEntry(Entry<OSXMemorySegment> entry) {
        this.entry = entry;
        this.timestamp = System.nanoTime();
    }

    public TimestampEntry(Entry<OSXMemorySegment> entry, long timestamp) {
        this.entry = entry;
        this.timestamp = timestamp;
    }

    public TimestampEntry(OSXMemorySegment key, OSXMemorySegment value, long timestamp) {
        this.entry = new BaseEntry<>(key, value);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public OSXMemorySegment key() {
        return entry.key();
    }

    @Override
    public OSXMemorySegment value() {
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
