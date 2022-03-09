package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Arrays;
import java.util.Comparator;

public class OSXMemorySegment implements Comparable<OSXMemorySegment> {
    private static final Comparator<MemorySegment> comparator = (MemorySegment m1, MemorySegment m2) -> {
        final long mismatch = m1.mismatch(m2);
        if (mismatch == -1) {
            return 0;
        }

        return Byte.compare(
                MemoryAccess.getByteAtOffset(m1, mismatch),
                MemoryAccess.getByteAtOffset(m2, mismatch)
        );
    };

    private final MemorySegment memorySegment;

    public OSXMemorySegment(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    @Override
    public int hashCode() {
        if (memorySegment.byteSize() <= Long.BYTES) {
            return Arrays.hashCode(memorySegment.toByteArray());
        }

        return Long.hashCode(MemoryAccess.getLong(memorySegment));
    }

    @Override
    public int compareTo(OSXMemorySegment o) {
        return comparator.compare(memorySegment, o.getMemorySegment());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof OSXMemorySegment) || hashCode() != obj.hashCode()) {
            return false;
        }

        return comparator.compare(memorySegment, ((OSXMemorySegment) obj).getMemorySegment()) == 0;
    }
}
