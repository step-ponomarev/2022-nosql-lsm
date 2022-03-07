package ru.mail.polis.stepanponomarev;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class UtilsTest {
    private static final OpenOption[] WRITE_OPEN_OPTIONS = {
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE
    };

    @Test
    public void toByteBufferTest() {
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
            ByteBuffer byteBuffer = Utils.toByteBuffer(i);
            Assertions.assertEquals(byteBuffer.getInt(), i);
        }
    }

    @Test
    public void readIntTest() throws URISyntaxException, IOException {
        final File file = new File(Path.of(LogTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath() + "/testFiles");
        file.mkdir();

        final Path path = new File(file.getAbsolutePath() + "/file.txt").toPath();
        try {
            final int num = 4;
            try (FileChannel fileChannel = FileChannel.open(path, WRITE_OPEN_OPTIONS)) {
                fileChannel.write(Utils.toByteBuffer(num));
            }

            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                Assertions.assertEquals(Utils.readInt(fileChannel, 0), num);
            }

        } finally {
            Files.walk(file.toPath())
                    .map(Path::toFile)
                    .forEach(File::delete);

            file.delete();
        }
    }
}
