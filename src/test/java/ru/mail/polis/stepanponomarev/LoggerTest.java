package ru.mail.polis.stepanponomarev;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
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
            Map<ByteBuffer, ByteBuffer> loadedData = log.read();
            loadedData.forEach((key, value) -> {
                Assertions.assertTrue(sourceData.containsKey(key));
                Assertions.assertEquals(sourceData.get(key), value);
            });
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

        Map<ByteBuffer, ByteBuffer> loadedData = log.read();
        Assertions.assertEquals(sourceData.size() / 2, loadedData.size());
        loadedData.forEach((key, value) -> {
            Assertions.assertTrue(sourceData.containsKey(key));
            Assertions.assertEquals(sourceData.get(key), value);
        });

        Files.walk(path)
                .map(Path::toFile)
                .forEach(File::delete);
        file.delete();
    }
}
