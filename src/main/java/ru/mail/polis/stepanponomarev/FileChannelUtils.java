package ru.mail.polis.stepanponomarev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

final class FileChannelUtils {
    private FileChannelUtils() {}

    public static final EnumSet<StandardOpenOption> APPEND_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
    );

    public static final EnumSet<StandardOpenOption> WRITE_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
    );

    public static final EnumSet<StandardOpenOption> READ_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.READ
    );

    public static ByteBuffer readByteBuffer(FileChannel fileChannel, long position, int keySize) throws IOException {
        if (!fileChannel.isOpen()) {
            throw new IllegalStateException("File channel is closed");
        }

        if (fileChannel.size() < position + keySize) {
            throw new IllegalArgumentException("Out of size position.");
        }

        ByteBuffer buffer = ByteBuffer.allocate(keySize);
        fileChannel.read(buffer, position);
        buffer.position(0);

        return buffer;
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
        buffer.position(0);

        return buffer.getInt();
    }
}
