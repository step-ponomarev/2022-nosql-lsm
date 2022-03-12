package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public final class Utils {
    public static final int TOMBSTONE_TAG = -1;

    private Utils() {
    }

    public static long sizeOf(EntryWithTime entry) {
        final OSXMemorySegment key = entry.key();
        final OSXMemorySegment value = entry.value();


        return key.size() + (value == null ? 0 : value.size()) + Long.BYTES;
    }

    public static long flush(EntryWithTime entry, MemorySegment memorySegment, long offset) {
        long writeOffset = offset;
        final MemorySegment key = entry.key().getMemorySegment();
        final long keySize = key.byteSize();
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
}
