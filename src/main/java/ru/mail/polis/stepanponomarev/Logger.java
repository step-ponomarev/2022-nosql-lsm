package ru.mail.polis.stepanponomarev;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Logger implements Closeable {
    private static final int tombstoneTag = -1;
    private static final String fileName = "lsm.log";

    private static final OpenOption[] writeOpenOptions = {
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
    };

    private final Path path;
    private FileChannel writeFileChannel;

    public Logger(Path path) throws IOException {
        this.path = new File(path.toAbsolutePath() + "/" + fileName).toPath();

        if (path.toFile().exists()) {
            this.writeFileChannel = FileChannel.open(this.path, writeOpenOptions);
        }
    }

    public void append(ByteBuffer keySrc, ByteBuffer valueSrc) throws IOException {
        if (keySrc == null) {
            throw new NullPointerException("Key can't be null.");
        }

        FileChannelUtils.append(writeFileChannel, keySrc, valueSrc, tombstoneTag);
    }

    public Map<ByteBuffer, ByteBuffer> read() throws IOException {
        if (!path.toFile().exists()) {
            return Collections.emptyMap();
        }

        try (final FileChannel readFileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            final Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
            final long size = readFileChannel.size();

            long currentPosition = readFileChannel.position();
            while (currentPosition != size) {
                int keySize = FileChannelUtils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                ByteBuffer key = FileChannelUtils.readByteBuffer(readFileChannel, currentPosition, keySize);
                currentPosition += keySize;

                int valueSize = FileChannelUtils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                if (valueSize == tombstoneTag) {
                    data.remove(key);
                    continue;
                }

                ByteBuffer value = FileChannelUtils.readByteBuffer(readFileChannel, currentPosition, valueSize);
                currentPosition += valueSize;

                data.put(key, value);
            }

            return data;
        }
    }

    public void clear() throws IOException {
        writeFileChannel.close();
        path.toFile().delete();
        writeFileChannel = FileChannel.open(path, writeOpenOptions);
    }

    @Override
    public void close() throws IOException {
        writeFileChannel.close();
    }
}
