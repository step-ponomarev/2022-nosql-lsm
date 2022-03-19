package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

public abstract class Utils {
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
    
    public static int compare(MemorySegment key1, MemorySegment key2) {
        return COMPARATOR.compare(key1, key2);
    }
}
