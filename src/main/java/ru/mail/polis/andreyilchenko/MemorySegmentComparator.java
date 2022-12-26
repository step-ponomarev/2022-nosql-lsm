package ru.mail.polis.andreyilchenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {

    public static final Comparator<MemorySegment> INSTANCE = new MemorySegmentComparator();

    private MemorySegmentComparator() {
    }

    @Override
    public int compare(MemorySegment m1, MemorySegment m2) {
        long firstMismatch = m1.mismatch(m2);
        if (firstMismatch == m2.byteSize()) {
            return 1;
        }
        if (firstMismatch == m1.byteSize()) {
            return -1;
        }
        if (firstMismatch == -1) {
            return 0;
        }
        return Byte.compareUnsigned(
                MemoryAccess.getByteAtOffset(m1, firstMismatch),
                MemoryAccess.getByteAtOffset(m2, firstMismatch)
        );
    }
}
