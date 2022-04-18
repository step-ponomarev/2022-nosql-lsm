package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.store.Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private static final long MEMORY_BYTE_LIMIT = 4000_000;

    private final Storage storage;
    private final AtomicLong sizeBytes;
    private final ExecutorService executorService;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.storage = new Storage(path);
        this.sizeBytes = new AtomicLong(0);
        this.executorService = Executors.newFixedThreadPool(2);
    }

    @Override
    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return new TombstoneSkipIterator<>(storage.get(from, to));
    }

    @Override
    public TimestampEntry get(MemorySegment key) throws IOException {
        return storage.get(key);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        storage.upsert(entry);

        final long newSizeBytes = sizeBytes.addAndGet(entry.getSizeBytes());
        if (newSizeBytes >= MEMORY_BYTE_LIMIT) {
            synchronized (this) {
                if (sizeBytes.get() >= MEMORY_BYTE_LIMIT) {
                    executorService.execute(() -> {
                        try {
                            storage.flush(entry.getTimestamp());
                        } catch (IOException e) {
                            throw new IllegalStateException("Something wrong with flush", e);
                        }
                    });
                    sizeBytes.addAndGet(-newSizeBytes);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        storage.close();
        executorService.shutdown();
    }

    @Override
    public void compact() throws IOException {
        final long timestamp = System.currentTimeMillis();
        executorService.execute(() -> {
            try {
                storage.compact(timestamp);
            } catch (IOException e) {
                throw new IllegalStateException("Something wrong with compact", e);
            }
        });
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.currentTimeMillis();
        storage.flush(timestamp);
    }
}
