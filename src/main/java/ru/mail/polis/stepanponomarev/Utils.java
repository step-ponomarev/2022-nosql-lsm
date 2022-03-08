package ru.mail.polis.stepanponomarev;

import java.nio.ByteBuffer;

class Utils {
    public static ByteBuffer toByteBuffer(int num) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(num);
        buffer.position(0);

        return buffer;
    }
}
