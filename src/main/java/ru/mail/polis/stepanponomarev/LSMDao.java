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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private static final long MEMORY_BYTE_LIMIT = 4000_000;

    private final Object upsertMonitorObject = new Object();

    private final Storage storage;
    private final AtomicLong sizeBytes;
    private final ExecutorService executorService;

    private final Lock flushCompactLock;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        this.storage = new Storage(path);
        this.sizeBytes = new AtomicLong(0);
        this.executorService = Executors.newFixedThreadPool(2);
        this.flushCompactLock = new ReentrantLock();
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
            synchronized (upsertMonitorObject) {
                if (sizeBytes.get() >= MEMORY_BYTE_LIMIT) {
                    executorService.execute(
                            new BlockingTask(() -> storage.flush(entry.getTimestamp()))
                    );
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
        executorService.execute(
                new BlockingTask(() -> storage.compact(System.currentTimeMillis()))
        );
    }

    @Override
    public void flush() throws IOException {
        flushCompactLock.lock();

        final long timestamp = System.currentTimeMillis();
        try {
            storage.flush(timestamp);
        } finally {
            flushCompactLock.unlock();
        }
    }

    private class BlockingTask implements Runnable {
        final Task task;

        private interface Task {
            void preformTask() throws Exception;
        }

        private BlockingTask(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            flushCompactLock.lock();

            try {
                task.preformTask();
            } catch (Exception e) {
                throw new IllegalStateException("Task failed", e);
            } finally {
                flushCompactLock.unlock();
            }
        }
    }
}
