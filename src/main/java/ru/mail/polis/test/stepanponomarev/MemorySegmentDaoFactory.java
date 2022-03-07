package ru.mail.polis.test.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.LmsDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.charset.StandardCharsets;

@DaoFactory
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new LmsDao();
    }

    @Override
    public String toString(MemorySegment data) {
        if (data == null) {
            return null;
        }

        if (data.byteSize() == 0) {
            throw new IllegalArgumentException("Buffer should have array");
        }

        return new String(data.toByteArray(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }

        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> entry) {
        return new BaseEntry<>(entry.key(), entry.value());
    }
}
