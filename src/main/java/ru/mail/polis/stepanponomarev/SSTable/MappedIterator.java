package ru.mail.polis.stepanponomarev.SSTable;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

class MappedIterator implements Iterator<Entry<ByteBuffer>> {
    final MappedByteBuffer mappedByteBuffer;

    public MappedIterator(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }

    @Override
    public boolean hasNext() {
        return this.mappedByteBuffer.position() != this.mappedByteBuffer.limit();
    }

    @Override
    public Entry<ByteBuffer> next() {
        final int keySize = mappedByteBuffer.getInt();
        final ByteBuffer key = mappedByteBuffer.slice(mappedByteBuffer.position(), keySize);
        mappedByteBuffer.position(mappedByteBuffer.position() + keySize);


        final int valueSize = mappedByteBuffer.getInt();
        if (valueSize == Utils.TOMBSTONE_TAG) {
            return new BaseEntry<>(key, null);
        }

        final ByteBuffer value = mappedByteBuffer.slice(mappedByteBuffer.position(), valueSize);
        mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);

        return new BaseEntry<>(key, value);
    }
}