package ru.mail.polis.stepanponomarev.log;

import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

public final class AsyncLogger {
    private final CommitLog commitLog;
    private final ConcurrentLinkedQueue<LogEntry> log;

    private static class LogEntry {
        private final long timestamp;
        private final Entry<OSXMemorySegment> entry;

        public LogEntry(Entry<OSXMemorySegment> entry, long timestamp) {
            this.timestamp = timestamp;
            this.entry = entry;
        }
    }

    public AsyncLogger(Path path, long size) throws IOException {
        this.commitLog = new CommitLog(path, size);
        this.log = new ConcurrentLinkedQueue<>();

        Executors.newSingleThreadExecutor().submit(() -> {
            final LogEntry timedLog = log.remove();

            try {
                commitLog.log(timedLog.entry);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void log(Entry<OSXMemorySegment> entry, long timestamp) {
        log.add(new LogEntry(entry, timestamp));
    }

    public void clear(long timestamp) {
        log.removeIf(e -> e.timestamp < timestamp);
        commitLog.clean();
    }

    public Iterator<Entry<OSXMemorySegment>> load() {
        return commitLog.load();
    }
}
