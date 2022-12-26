package ru.mail.polis.test.stepanponomarev;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.MemorySegmentDao;


import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.test.DaoFactory;

@DaoFactory(stage = 4)
public class LSMDaoFactory implements DaoFactory.Factory<MemorySegment, TimestampEntry> {

    @Override
    public Dao<MemorySegment, TimestampEntry> createDao(Config config) throws IOException {
        return new MemorySegmentDao(
            config.basePath(),
            config.flushThresholdBytes()
        );
    }

    @Override
    public String toString(MemorySegment data) {
        if (data == null) {
            return null;
        }

        return StandardCharsets.UTF_8.decode(data.asByteBuffer()).toString();
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }

        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public TimestampEntry fromBaseEntry(Entry<MemorySegment> entry) {
        return new TimestampEntry(entry);
    }
}
