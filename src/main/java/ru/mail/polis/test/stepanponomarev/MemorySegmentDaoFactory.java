package ru.mail.polis.test.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.ComparableMemorySegmentWrapper;
import ru.mail.polis.stepanponomarev.LmsDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

@DaoFactory(stage = 2)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> {

    @Override
    public Dao<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> createDao(Config config) throws IOException {
        return new LmsDao(config.basePath());
    }

    @Override
    public String toString(ComparableMemorySegmentWrapper data) {
        if (data == null) {
            return null;
        }

        if (data.getMemorySegment().byteSize() == 0) {
            throw new IllegalArgumentException("Buffer should have array");
        }

        return data.toString();
    }

    @Override
    public ComparableMemorySegmentWrapper fromString(String data) {
        return ComparableMemorySegmentWrapper.from(data);
    }

    @Override
    public Entry<ComparableMemorySegmentWrapper> fromBaseEntry(Entry<ComparableMemorySegmentWrapper> entry) {
        return new BaseEntry<>(entry.key(), entry.value());
    }
}
