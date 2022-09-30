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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private final Storage storage;
    private final AtomicLong sizeBytes;
    private final ExecutorService executorService;
    private final long limitBytes;

    public LSMDao(Path path, long limitBytes) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.limitBytes = limitBytes;
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
        final long newSizeBytes = sizeBytes.addAndGet(entry.getSizeBytes());
        storage.upsert(entry);
        if (newSizeBytes < limitBytes) {
            return;
        }

        synchronized (this) {
            sizeBytes.addAndGet(-newSizeBytes);
            if (sizeBytes.get() < limitBytes) {
                return;
            }

            executorService.execute(() -> handleFlushAsync(newSizeBytes));
        }
    }

    private void handleFlushAsync(long newSizeBytes) {
        try {
            final long flushedSizeBytes = storage.flush(System.currentTimeMillis());

            if (flushedSizeBytes > 0) {
                final long diff = flushedSizeBytes - newSizeBytes;
                sizeBytes.addAndGet(-diff);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        flush();
        storage.close();
    }

    @Override
    public void compact() throws IOException {
        executorService.execute(
                () -> {
                    try {
                        storage.compact(System.currentTimeMillis());
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
        );
    }

    @Override
    public void flush() throws IOException {
        storage.flush(
                System.currentTimeMillis()
        );
    }
}
