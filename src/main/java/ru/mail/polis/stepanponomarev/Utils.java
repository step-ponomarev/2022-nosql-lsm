package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

final class Utils {
    private Utils() {}

    public final static Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR = (MemorySegment o1, MemorySegment o2) -> {
        if (o1 == o2) {
            return 0;
        }

        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }

        byte byteAtOffset1 = MemoryAccess.getByteAtOffset(o1, mismatch);
        byte byteAtOffset2 = MemoryAccess.getByteAtOffset(o2, mismatch);

        return byteAtOffset1 < byteAtOffset2 ? -1 : 1;
    };
}
