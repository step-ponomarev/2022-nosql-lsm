package ru.mail.polis.stepanponomarev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class Utils {
    public static ByteBuffer toByteBuffer(int num) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(num);
        buffer.position(0);

        return buffer;
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
