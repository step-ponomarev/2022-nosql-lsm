package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.Comparator;

public class ComparableMemorySegmentWrapperTest {
    private static final Comparator<MemorySegment> memorySegmentComparator = ComparableMemorySegmentWrapper.MEMORY_SEGMENT_COMPARATOR;

    @Test
    public void memorySegmentComparatorTest() {
        final String str = "a";
        Assertions.assertEquals(0, memorySegmentComparator.compare(
                ComparableMemorySegmentWrapper.from(str).getMemorySegment(),
                ComparableMemorySegmentWrapper.from(str).getMemorySegment())
        );

        final MemorySegment minValueMemorySegment = ComparableMemorySegmentWrapper.from("a").getMemorySegment();
        final MemorySegment midValueMemorySegment = ComparableMemorySegmentWrapper.from("b").getMemorySegment();
        final MemorySegment maxValueMemorySegment = ComparableMemorySegmentWrapper.from("c").getMemorySegment();

        Assertions.assertTrue(memorySegmentComparator.compare(minValueMemorySegment, midValueMemorySegment) < 0);
        Assertions.assertTrue(memorySegmentComparator.compare(midValueMemorySegment, maxValueMemorySegment) < 0);
        Assertions.assertTrue(memorySegmentComparator.compare(minValueMemorySegment, maxValueMemorySegment) < 0);
    }

    @Test
    public void memorySegmentWrapperEqualsTest() {
        final String str = "a";

        Assertions.assertEquals(
                ComparableMemorySegmentWrapper.from(str),
                ComparableMemorySegmentWrapper.from(str)
        );
    }
}
