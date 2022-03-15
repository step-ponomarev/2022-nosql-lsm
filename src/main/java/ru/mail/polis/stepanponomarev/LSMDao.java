package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.log.LoggerAhead;
import ru.mail.polis.stepanponomarev.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LSMDao implements Dao<OSXMemorySegment, TimestampEntry> {
    private static final long MAX_MEM_TABLE_SIZE_BYTES = (long) 2.5E8;

    private final Store store;
    private final LoggerAhead loggerAhead;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + "is not exist");
        }

        loggerAhead = new LoggerAhead(path, MAX_MEM_TABLE_SIZE_BYTES);
        store = new Store(path, loggerAhead.load());
    }

    @Override
    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        return store.get(from, to);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        loggerAhead.log(entry);
        store.put(entry);

        if (store.getSizeBytes() >= MAX_MEM_TABLE_SIZE_BYTES) {
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        loggerAhead.close();
        flush();
    }

    @Override
    //TODO: Асинхронный флаш
    public void flush() throws IOException {
        final long timestamp = System.nanoTime();
        store.flush(timestamp);
        loggerAhead.clear(timestamp);
    }
}
