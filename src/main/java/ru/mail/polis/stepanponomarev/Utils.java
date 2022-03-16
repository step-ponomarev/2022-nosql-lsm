package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class Utils {
    public static final int TOMBSTONE_TAG = -1;

    public static final Comparator<MemorySegment> COMPARATOR = (MemorySegment m1, MemorySegment m2) -> {
        final long mismatch = m1.mismatch(m2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == m1.byteSize()) {
            return -1;
        }

        if (mismatch == m2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                MemoryAccess.getByteAtOffset(m1, mismatch),
                MemoryAccess.getByteAtOffset(m2, mismatch)
        );
    };

    private Utils() {
    }

    public static SortedMap<MemorySegment, TimestampEntry> createMap() {
        return new ConcurrentSkipListMap<>(COMPARATOR);
    }

    public static int compare(MemorySegment segment, MemorySegment segment2) {
        return COMPARATOR.compare(segment, segment2);
    }

    public static long sizeOf(TimestampEntry entry) {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();

        return key.byteSize() + (value == null ? 0 : value.byteSize()) + Long.BYTES;
    }

    public static long flush(TimestampEntry entry, MemorySegment memorySegment, long offset) {
        final MemorySegment key = entry.key();
        final long keySize = key.byteSize();

        long writeOffset = offset;
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, keySize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, keySize).copyFrom(key);
        writeOffset += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, entry.getTimestamp());
        writeOffset += Long.BYTES;

        final MemorySegment value = entry.value();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, writeOffset, TOMBSTONE_TAG);
            return writeOffset + Long.BYTES;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, valueSize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, valueSize).copyFrom(value);
        writeOffset += valueSize;

        return writeOffset;
    }

    public static Iterator<TimestampEntry> slice(
            SortedMap<MemorySegment, TimestampEntry> store,
            MemorySegment from,
            MemorySegment to
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
