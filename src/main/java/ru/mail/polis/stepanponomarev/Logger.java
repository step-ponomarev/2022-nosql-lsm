package ru.mail.polis.stepanponomarev;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Logger implements Closeable {
    private static final String fileName = "lsm.log";

    private Path path;
    private FileChannel writeFileChannel;

    public Logger(Path basePath) throws IOException {
        if (!basePath.toFile().exists()) {
            return;
        }
        path = new File(basePath.toAbsolutePath() + "/" + fileName).toPath();
        writeFileChannel = FileChannel.open(path, FileChannelUtils.APPEND_OPEN_OPTIONS);
    }

    public void append(ByteBuffer keySrc, ByteBuffer valueSrc) throws IOException {
        if (keySrc == null) {
            throw new NullPointerException("Key can't be null.");
        }

        if (!writeFileChannel.isOpen()) {
            throw new IllegalStateException("FileChannel should be opened.");
        }

        ByteBuffer key = keySrc.duplicate();
        ByteBuffer value = valueSrc == null ? null : valueSrc.duplicate();
        ByteBuffer[] byteBuffers = {
                Utils.toByteBuffer(key.remaining()),
                key,
                value == null ? Utils.toByteBuffer(Utils.TOMBSTONE_TAG) : Utils.toByteBuffer(value.remaining())
        };

        writeFileChannel.write(byteBuffers);
        if (value != null) {
            writeFileChannel.write(value);
        }
    }

    public Map<ByteBuffer, ByteBuffer> read() throws IOException {
        if (path == null || !path.toFile().exists()) {
            return Collections.emptyMap();
        }

        try (final FileChannel readFileChannel = FileChannel.open(path, FileChannelUtils.READ_OPEN_OPTIONS)) {
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

                if (valueSize == Utils.TOMBSTONE_TAG) {
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
        writeFileChannel = FileChannel.open(path, FileChannelUtils.APPEND_OPEN_OPTIONS);
    }

    @Override
    public void close() throws IOException {
        if (writeFileChannel == null) {
            return;
        }

        writeFileChannel.close();
    }
}
