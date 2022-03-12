package ru.mail.polis.test.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.EntryWithTime;
import ru.mail.polis.stepanponomarev.LsmDao;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@DaoFactory(stage = 2)
public class LsmDaoFactory implements DaoFactory.Factory<OSXMemorySegment, EntryWithTime> {

    @Override
    public Dao<OSXMemorySegment, EntryWithTime> createDao(Config config) throws IOException {
        final Path path = config.basePath();
        if (Files.notExists(path)) {
            Files.createDirectory(path);
        }

        return new LsmDao(path);
    }

    @Override
    public String toString(OSXMemorySegment data) {
        if (data == null) {
            return null;
        }

        return StandardCharsets.UTF_8.decode(data.getMemorySegment().asByteBuffer()).toString();
    }

    @Override
    public OSXMemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }

        return new OSXMemorySegment(MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public EntryWithTime fromBaseEntry(Entry<OSXMemorySegment> baseEntry) {
        return new EntryWithTime(baseEntry, System.nanoTime());
    }
}
