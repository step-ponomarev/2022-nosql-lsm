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


    private final Path logPath;
    private FileChannel writeFileChannel;

    public Logger(Path path) throws IOException {
        logPath = new File(path.toAbsolutePath() + "/" + FILE_NAME).toPath();
        writeFileChannel = FileChannel.open(logPath, WRITE_OPEN_OPTIONS);
    }

    public void append(ByteBuffer keySrc, ByteBuffer valueSrc) throws IOException {
        if (keySrc == null) {
            throw new NullPointerException("Key can't be null.");
        }

        if (!writeFileChannel.isOpen()) {
            writeFileChannel = FileChannel.open(logPath, WRITE_OPEN_OPTIONS);
        }

        ByteBuffer key = keySrc.duplicate();
        ByteBuffer value = valueSrc == null ? null : valueSrc.duplicate();

        //TODO: Очень неявный контракт
        ByteBuffer valueSizeByteBuffer = value == null ? Utils.toByteBuffer(TOMBSTONE_TAG) : Utils.toByteBuffer(value.remaining());

        ByteBuffer[] byteBuffers = {
                Utils.toByteBuffer(key.remaining()),
                key,
                valueSizeByteBuffer
        };

        writeFileChannel.write(byteBuffers);
        if (value != null) {
            writeFileChannel.write(value);
        }
    }

    public Map<ByteBuffer, ByteBuffer> read() throws IOException {
        if (!logPath.toFile().exists()) {
            return Collections.emptyMap();
        }

        try (final FileChannel readFileChannel = FileChannel.open(logPath, StandardOpenOption.READ)) {
            final Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
            final long size = readFileChannel.size();

            long currentPosition = readFileChannel.position();
            while (currentPosition != size) {
                int keySize = Utils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                ByteBuffer key = Utils.readByteBuffer(readFileChannel, currentPosition, keySize);
                currentPosition += keySize;

                int valueSize = Utils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                if (valueSize == TOMBSTONE_TAG) {
                    data.remove(key);
                    currentPosition += Integer.BYTES;
                    continue;
                }

                ByteBuffer value = Utils.readByteBuffer(readFileChannel, currentPosition, valueSize);
                currentPosition += valueSize;

                data.put(key, value);
            }

            return data;
        }
    }

    public void clean() throws IOException {
        writeFileChannel.close();
        logPath.toFile().delete();
    }

    @Override
    public void close() throws IOException {
        writeFileChannel.close();
    }
}
