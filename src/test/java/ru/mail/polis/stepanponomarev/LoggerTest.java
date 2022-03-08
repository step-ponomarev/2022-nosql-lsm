package ru.mail.polis.stepanponomarev;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.mail.polis.Entry;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LoggerTest {
    @Test
    public void testLog() throws IOException, URISyntaxException {
        final File file = new File(Path.of(LoggerTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath() + "/testFiles");
        file.mkdir();

        final Path path = file.toPath();

        Logger log = new Logger(path);
        final int max = 100_000;

        Map<ByteBuffer, ByteBuffer> sourceData = new HashMap<>(max);
        for (int i = 0; i < max; i++) {
            ByteBuffer key = ByteBuffer.wrap(("_" + i + "_").getBytes(StandardCharsets.UTF_8));
            ByteBuffer value = ByteBuffer.wrap(("_" + (max - i) + "_").getBytes(StandardCharsets.UTF_8));

            sourceData.put(key, value);
            log.append(key, value);
        }

        try {
            Iterator<Entry<ByteBuffer>> read = log.get();
            while (read.hasNext()) {
                Entry<ByteBuffer> next = read.next();

                Assertions.assertTrue(sourceData.containsKey(next.key()));
                Assertions.assertEquals(sourceData.get(next.key()), next.value());
            }
        } finally {
            Files.walk(path)
                    .map(Path::toFile)
                    .forEach(File::delete);
            file.delete();
        }
    }

    @Test
    public void testFilterOutTombstone() throws IOException, URISyntaxException {
        final File file = new File(Path.of(LoggerTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath() + "/testFiles");
        file.mkdir();

        final Path path = file.toPath();

        Logger log = new Logger(path);
        final int max = 100_000;

        Map<ByteBuffer, ByteBuffer> sourceData = new HashMap<>(max);
        for (int i = 0; i < max; i++) {
            ByteBuffer key = ByteBuffer.wrap(("_" + i + "_").getBytes(StandardCharsets.UTF_8));
            ByteBuffer value = i % 2 == 0 ? null : ByteBuffer.wrap(("_" + (max - i) + "_").getBytes(StandardCharsets.UTF_8));

            sourceData.put(key, value);
            log.append(key, value);
        }

        Iterator<Entry<ByteBuffer>> read = log.get();
        int size = 0;
        while (read.hasNext()) {
            Entry<ByteBuffer> next = read.next();

            size++;
            Assertions.assertTrue(sourceData.containsKey(next.key()));
            Assertions.assertEquals(sourceData.get(next.key()), next.value());
        }

        Assertions.assertEquals(sourceData.size() / 2, size);

        Files.walk(path)
                .map(Path::toFile)
                .forEach(File::delete);
        file.delete();
    }
}
