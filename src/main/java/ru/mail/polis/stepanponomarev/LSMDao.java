package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.store.Store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private final Store store;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        store = new Store(path);
    }

    @Override
    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return new TombstoneSkipIterator<>(store.get(from, to));
    }

    @Override
    public TimestampEntry get(MemorySegment key) throws IOException {
        return store.get(key);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        store.put(entry);
    }

    @Override
    public void close() throws IOException {
        flush();
        store.close();
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.currentTimeMillis();
        store.flush(timestamp);
    }
}
