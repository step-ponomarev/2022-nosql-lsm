package ru.mail.polis.test.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment data) {
        return StandardCharsets.UTF_8.decode(data.asByteBuffer()).toString();
    }

    @Override
    public MemorySegment fromString(String data) {
        return MemorySegment.ofByteBuffer(StandardCharsets.UTF_8.encode(data));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> entry) {
        return entry;
    }
}
