package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.store.Store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private final Store store;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }
        
        store = new Store(path, Collections.emptyIterator());
    }

    @Override
    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        try {
            return store.get(from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public TimestampEntry get(MemorySegment key) throws IOException {
        lock.readLock().lock();
        try {
            return store.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(TimestampEntry entry) {
        lock.readLock().lock();
        try {
            store.put(entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        store.close();
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.currentTimeMillis();
        
        lock.writeLock().lock();
        try {
            store.flush(timestamp);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
