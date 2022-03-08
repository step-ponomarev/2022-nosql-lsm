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
    private static final int TOMBSTONE_TAG = -1;

    private static final String FILE_NAME = "lsm.txt";
    private static final OpenOption[] WRITE_OPEN_OPTIONS = {
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
    };

    private final Path path;
    private FileChannel writeFileChannel;

    public Logger(Path path) throws IOException {
        this.path = new File(path.toAbsolutePath() + "/" + FILE_NAME).toPath();
        writeFileChannel = FileChannel.open(this.path, WRITE_OPEN_OPTIONS);
    }

    public void append(ByteBuffer keySrc, ByteBuffer valueSrc) throws IOException {
        if (keySrc == null) {
            throw new NullPointerException("Key can't be null.");
        }

        if (!writeFileChannel.isOpen()) {
            writeFileChannel = FileChannel.open(path, WRITE_OPEN_OPTIONS);
        }

        FileChannelUtils.append(writeFileChannel, keySrc, valueSrc, TOMBSTONE_TAG);
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

                if (valueSize == TOMBSTONE_TAG) {
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
    }

    @Override
    public void close() throws IOException {
        writeFileChannel.close();
    }
}
