package ru.mail.polis.stepanponomarev.log;

import ru.mail.polis.stepanponomarev.TimestampEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AsyncLogger implements Closeable {
    private final CommitLog commitLog;
    private final ConcurrentLinkedQueue<TimestampEntry> log;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ExecutorService executorService;

    public AsyncLogger(Path path, long size) throws IOException {
        this.commitLog = new CommitLog(path, size);
        this.log = new ConcurrentLinkedQueue<>();

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
                    while (closed.get() || !log.isEmpty()) {
                        final TimestampEntry timedLog = log.remove();

                        try {
                            commitLog.log(timedLog);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
        );
    }

    public void log(TimestampEntry entry) {
        log.add(entry);
    }

    public void clear(long timestamp) throws IOException {
        log.removeIf(e -> e.getTimestamp() < timestamp);
        commitLog.clean();
    }

    @Override
    public void close() throws IOException {
        closed.set(true);

        executorService.shutdown();
        try {
            try {
                if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                    throw new IOException("We are waiting too loong.");
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw e;
            }
        } catch (InterruptedException e) {
            throw new IOException("Very strange unexpected exception.", e);
        }
    }

    public Iterator<TimestampEntry> load() {
        return commitLog.load();
    }
}
