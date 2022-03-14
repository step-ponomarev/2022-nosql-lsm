package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;

public final class Utils {
    public static final int TOMBSTONE_TAG = -1;

    private Utils() {
    }

    public static long sizeOf(TimestampEntry entry) {
        final OSXMemorySegment key = entry.key();
        final OSXMemorySegment value = entry.value();

        return key.size() + (value == null ? 0 : value.size()) + Long.BYTES;
    }

    public static long flush(TimestampEntry entry, MemorySegment memorySegment, long offset) {
        final MemorySegment key = entry.key().getMemorySegment();
        final long keySize = key.byteSize();

        long writeOffset = offset;
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, keySize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, keySize).copyFrom(key);
        writeOffset += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, entry.getTimestamp());
        writeOffset += Long.BYTES;

        final OSXMemorySegment value = entry.value();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, writeOffset, TOMBSTONE_TAG);
            return writeOffset + Long.BYTES;
        }

        final long valueSize = value.size();
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, valueSize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, valueSize).copyFrom(value.getMemorySegment());
        writeOffset += valueSize;

        return writeOffset;
    }

    public static Iterator<TimestampEntry> slice(
            SortedMap<OSXMemorySegment, TimestampEntry> store,
            OSXMemorySegment from,
            OSXMemorySegment to
    ) {
        if (store == null || store.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return store.values().iterator();
        }

        if (from == null) {
            return store.headMap(to).values().iterator();
        }

        if (to == null) {
            return store.tailMap(from).values().iterator();
        }

        return store.subMap(from, to).values().iterator();
    }
}
