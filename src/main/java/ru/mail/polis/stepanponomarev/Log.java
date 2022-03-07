package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Log implements Closeable {
    private static final String FILE_NAME = "lsm.txt";
    private static final OpenOption[] WRITE_OPEN_OPTIONS = {
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE
    };

    private final FileChannel fileChannel;
    private final Path logPath;

    public Log(Path path) throws IOException {
        logPath = new File(path.toAbsolutePath() + "/" + FILE_NAME).toPath();
        fileChannel = FileChannel.open(
                logPath,
                WRITE_OPEN_OPTIONS
        );
    }

    public void append(MemorySegment key, MemorySegment value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key can't be null.");
        }

        ByteBuffer keyByteBuffer = key.asByteBuffer();
        ByteBuffer keySizeByteBuffer = Utils.toByteBuffer(keyByteBuffer.remaining());

        //TODO: Очень неявный контракт
        ByteBuffer valueByteBuffer = value == null ? Utils.toByteBuffer(-1) : value.asByteBuffer();
        ByteBuffer valueSizeByteBuffer = value == null ? Utils.toByteBuffer(-1) : Utils.toByteBuffer(valueByteBuffer.remaining());

        ByteBuffer[] byteBuffers = {
                keySizeByteBuffer,
                keyByteBuffer,
                valueSizeByteBuffer,
                valueByteBuffer
        };

        fileChannel.write(byteBuffers);
    }

    public Iterator<Map.Entry<MemorySegment, MemorySegment>> read() throws IOException {
        try (final FileChannel readFileChannel = FileChannel.open(logPath, StandardOpenOption.READ)) {
            final Map<MemorySegment, MemorySegment> values = new HashMap<>();
            final long size = readFileChannel.size();

            long currentPosition = readFileChannel.position();
            while (currentPosition != size) {
                int keySize = Utils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                ByteBuffer key = Utils.readByteBuffer(readFileChannel, currentPosition, keySize);
                currentPosition += keySize;

                int valueSize = Utils.readInt(readFileChannel, currentPosition);
                currentPosition += Integer.BYTES;

                //TODO: Очень неявный контракт
                // если размер valueSize -1 я знаю, что записал туда инт, его можно просто скипнуть
                if (valueSize == -1) {
                    values.remove(key);
                    currentPosition += Integer.BYTES;
                    continue;
                }

                ByteBuffer value = Utils.readByteBuffer(readFileChannel, currentPosition, valueSize);
                currentPosition += valueSize;

                values.put(MemorySegment.ofByteBuffer(key), MemorySegment.ofByteBuffer(value));
            }

            return values.entrySet().iterator();
        }
    }

    public void clean() {
        if (fileChannel.isOpen()) {
            throw new IllegalStateException("FileChannel should be closed");
        }

        logPath.toFile().delete();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
