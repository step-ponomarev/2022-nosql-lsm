package ru.mail.polis.stepanponomarev;

import java.nio.ByteBuffer;

class Utils {
    public static final int TOMBSTONE_TAG = -1;

    public static ByteBuffer toByteBuffer(int num) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(num);
        buffer.position(0);

        return buffer;
    }

    public static ByteBuffer toByteBuffer(long num) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(num);
        buffer.position(0);

        return buffer;
    }
}
