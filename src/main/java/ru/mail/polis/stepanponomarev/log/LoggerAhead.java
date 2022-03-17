package ru.mail.polis.stepanponomarev.log;

import ru.mail.polis.stepanponomarev.TimestampEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class LoggerAhead implements Closeable {
    private static final Logger log = Logger.getLogger(LoggerAhead.class.getSimpleName());

    private static final TimestampEntry CLOSE_SIGNAL = new TimestampEntry(null, -1);

    private final CommitLog commitLog;
    private final ExecutorService executorService;
    private final BlockingQueue<TimestampEntry> entryQueue;

    public LoggerAhead(Path path, long size) throws IOException {
        this.commitLog = new CommitLog(path, size);
        this.entryQueue = new LinkedBlockingQueue<>();

        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(this::execute);
        executorService.shutdown();
    }

    private void execute() {
        while (true) {
            try {
                final TimestampEntry timedLog = entryQueue.take();
                final boolean stopped = timedLog.equals(CLOSE_SIGNAL);
                if (stopped && entryQueue.isEmpty()) {
                    break;
                }

                if (stopped) {
                    entryQueue.put(CLOSE_SIGNAL);
                }

                if (!stopped) {
                    commitLog.log(timedLog);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void log(TimestampEntry entry) {
        entryQueue.add(entry);
    }

    public void clear(long timestamp) throws IOException {
        entryQueue.removeIf(e -> e.getTimestamp() < timestamp);
        commitLog.clean();
    }

    @Override
    public void close() throws IOException {
        log.info("STOPPING COMMIT LOGGER");

        try {
            entryQueue.put(CLOSE_SIGNAL);

            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IOException("We are waiting too long.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (Thread.interrupted()) {
                throw new IOException("Very strange unexpected exception", e);
            }
        } finally {
            commitLog.close();
        }

        log.info("COMMIT LOGGER IS STOPPED");
    }

    public Iterator<TimestampEntry> load() {
        return commitLog.load();
    }
}
