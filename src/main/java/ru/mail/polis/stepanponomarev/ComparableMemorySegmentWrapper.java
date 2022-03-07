package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

public final class ComparableMemorySegmentWrapper implements Comparable<ComparableMemorySegmentWrapper> {
    public final static Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR = (MemorySegment o1, MemorySegment o2) -> {
        if (o1.equals(o2)) {
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

    private final MemorySegment memorySegment;

    public ComparableMemorySegmentWrapper(MemorySegment memorySegment) {
        if (memorySegment == null) {
            throw new NullPointerException("MemorySegment can't be null.");
        }

        this.memorySegment = memorySegment;
    }

    @Override
    public String toString() {
        return new String(memorySegment.toByteArray(), StandardCharsets.UTF_8);
    }

    public static ComparableMemorySegmentWrapper from(String data) {
        if (data == null) {
            return null;
        }

        return new ComparableMemorySegmentWrapper(
                MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8))
        );
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(memorySegment.toByteArray());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (hashCode() != obj.hashCode() || !(obj instanceof ComparableMemorySegmentWrapper)) {
            return false;
        }

        ComparableMemorySegmentWrapper comparableMemorySegmentWrapper = (ComparableMemorySegmentWrapper) obj;

        return memorySegment.mismatch(comparableMemorySegmentWrapper.memorySegment) == -1;
    }

    @Override
    public int compareTo(ComparableMemorySegmentWrapper o) {
        return MEMORY_SEGMENT_COMPARATOR.compare(memorySegment, o.memorySegment);
    }
}
