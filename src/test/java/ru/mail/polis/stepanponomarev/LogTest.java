package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LogTest {
    @Test
    public void testLog() throws IOException, URISyntaxException {
        final File file = new File(Path.of(LogTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath() + "/testFiles");
        file.mkdir();

        final Path path = file.toPath();

        ComparableMemorySegmentWrapper from = ComparableMemorySegmentWrapper.from("4");
        MemorySegment memorySegment = from.getMemorySegment();
        ByteBuffer byteBuffer = memorySegment.asByteBuffer();

        int i1 = byteBuffer.remaining();
        ByteBuffer byteBuffer1 = Utils.toByteBuffer(byteBuffer.remaining());

        Assertions.assertEquals(i1, byteBuffer1.getInt());

        try (Log log = new Log(path)) {
            final String a = "a";
            final int max = 100;

            Map<ComparableMemorySegmentWrapper, ComparableMemorySegmentWrapper> keyValue = new HashMap<>(max);
            for (int i = 0; i < max; i++) {
                ComparableMemorySegmentWrapper key = ComparableMemorySegmentWrapper.from(a + "_" + i);
                ComparableMemorySegmentWrapper value = ComparableMemorySegmentWrapper.from(a + "_" + (max - i));

                keyValue.put(key, value);
                log.append(key.getMemorySegment(), value.getMemorySegment());
            }

            Iterator<Map.Entry<MemorySegment, MemorySegment>> read = log.read();
            Map<ComparableMemorySegmentWrapper, ComparableMemorySegmentWrapper> keyValue2 = new HashMap<>(max);
            while (read.hasNext()) {
                Map.Entry<MemorySegment, MemorySegment> next = read.next();

                keyValue2.put(new ComparableMemorySegmentWrapper(next.getKey()), new ComparableMemorySegmentWrapper(next.getValue()));
            }

            keyValue2.entrySet().forEach(e -> {
                Assertions.assertTrue(keyValue.containsKey(e.getKey()));
                Assertions.assertEquals(keyValue.get(e.getKey()), e.getValue());
            });
        } finally {
            Files.walk(path)
                    .map(Path::toFile)
                    .forEach(File::delete);
            file.delete();
        }
    }
}
