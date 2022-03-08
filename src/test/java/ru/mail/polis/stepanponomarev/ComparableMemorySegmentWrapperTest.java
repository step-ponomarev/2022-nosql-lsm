package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    @Test
    public void testCompareAfterReading() throws URISyntaxException, IOException {
        final File dir = new File(Path.of(LoggerTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath() + "/testFiles");
        dir.mkdir();
        final Path path = new File(dir.getAbsolutePath() + "/test.file").toPath();

        final MemorySegment minValueMemorySegment = ComparableMemorySegmentWrapper.from("key").getMemorySegment();
        ByteBuffer byteBuffer = minValueMemorySegment.asByteBuffer();

        try  {
            try(FileChannel fileChannel = FileChannel.open(path, new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE})) {
                fileChannel.write(byteBuffer);
            }

            try(FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer bb = ByteBuffer.allocate((int) fileChannel.size());
                fileChannel.read(bb);

                Assertions.assertEquals(byteBuffer, bb);
                Assertions.assertEquals(
                        new ComparableMemorySegmentWrapper(MemorySegment.ofByteBuffer(byteBuffer)),
                        new ComparableMemorySegmentWrapper(MemorySegment.ofByteBuffer(bb)));

            }
        } finally {
            Files.walk(dir.toPath())
                    .map(Path::toFile)
                    .forEach(File::delete);

            dir.delete();
        }
    }
}
