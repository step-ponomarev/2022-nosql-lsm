package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.io.Closeable;

final class Index implements Closeable {
    public static final String FILE_NAME = "sstable.index";

    private final MemorySegment indexMemorySegment;

    public Index(MemorySegment mappedIndex) {
        this.indexMemorySegment = mappedIndex;
    }

    public long getPositionByIndex(long index) {
        return MemoryAccess.getLongAtIndex(indexMemorySegment, index);
    }

    public int getPositionAmount() {
        return (int) indexMemorySegment.byteSize() / Long.BYTES;
    }

    @Override
    public void close() {
        indexMemorySegment.scope().close();
    }
}
