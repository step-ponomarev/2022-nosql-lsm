package ru.mail.polis.stepanponomarev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelUtils {
    public static void append(FileChannel fileChannel, ByteBuffer keySrc, ByteBuffer valueSrc, int tombstoneTag) throws IOException {
        if (keySrc == null) {
            throw new NullPointerException("Key can't be null.");
        }

        ByteBuffer key = keySrc.duplicate();
        ByteBuffer value = valueSrc == null ? null : valueSrc.duplicate();
        ByteBuffer[] byteBuffers = {
                Utils.toByteBuffer(key.remaining()),
                key,
                value == null ? Utils.toByteBuffer(tombstoneTag) : Utils.toByteBuffer(value.remaining())
        };

        fileChannel.write(byteBuffers);
        if (value != null) {
            fileChannel.write(value);
        }
    }

    public static ByteBuffer readByteBuffer(FileChannel fileChannel, long position, int keySize) throws IOException {
        if (!fileChannel.isOpen()) {
            throw new IllegalStateException("File channel is closed");
        }

        if (fileChannel.size() < position + keySize) {
            throw new IllegalArgumentException("Out of size position.");
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        fileChannel.read(byteBuffer, position);
        byteBuffer.position(0);

        return byteBuffer;
    }

    public static int readInt(FileChannel fileChannel, long position) throws IOException {
        if (!fileChannel.isOpen()) {
            throw new IllegalStateException("File channel is closed");
        }

        if (fileChannel.size() < position + Integer.BYTES) {
            throw new IllegalArgumentException("Out of size position.");
        }

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer, position);
        buffer.flip();

        return buffer.getInt();
    }
}
