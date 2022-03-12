package ru.mail.polis.stepanponomarev.iterator;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class MappedIterator implements Iterator<TimestampEntry> {
    private final MemorySegment memorySegment;
    private long position;

    public MappedIterator(MemorySegment segment) {
        memorySegment = segment;
        position = 0;
    }

    @Override
    public boolean hasNext() {
        return memorySegment.byteSize() != position;
    }

    @Override
    public TimestampEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final long keySize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        final MemorySegment key = memorySegment.asSlice(position, keySize);
        position += keySize;

        final long timestamp = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        final long valueSize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        if (valueSize == Utils.TOMBSTONE_TAG) {
            return new TimestampEntry(new OSXMemorySegment(key), null, timestamp);
        }

        MemorySegment value = memorySegment.asSlice(position, valueSize);
        position += valueSize;

        return new TimestampEntry(new OSXMemorySegment(key), new OSXMemorySegment(value), timestamp);
    }
}
