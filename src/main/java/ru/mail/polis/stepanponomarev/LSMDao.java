package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.log.LoggerAhead;
import ru.mail.polis.stepanponomarev.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private static final long MAX_MEMTABLE_SIZE_BYTES = (long) 2.5E8;

    private final Store store;
    private final LoggerAhead loggerAhead;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + "is not exist");
        }

        loggerAhead = new LoggerAhead(path, MAX_MEMTABLE_SIZE_BYTES);
        store = new Store(path, loggerAhead.load());
    }

    @Override
    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return store.get(from, to);
    }

    @Override
    public TimestampEntry get(MemorySegment key) throws IOException {
        return store.get(key);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        loggerAhead.log(entry);
        store.put(entry);

        if (store.getSizeBytes() >= MAX_MEMTABLE_SIZE_BYTES) {
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        store.close();
        loggerAhead.close();
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.nanoTime();
        store.flush(timestamp);
        loggerAhead.clear(timestamp);
    }
}
