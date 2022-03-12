package ru.mail.polis.stepanponomarev.log;

import ru.mail.polis.stepanponomarev.EntryWithTime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

public final class AsyncLogger {
    private final CommitLog commitLog;
    private final ConcurrentLinkedQueue<EntryWithTime> log;

    public AsyncLogger(Path path, long size) throws IOException {
        this.commitLog = new CommitLog(path, size);
        this.log = new ConcurrentLinkedQueue<>();

        Executors.newSingleThreadExecutor().submit(() -> {
            final EntryWithTime timedLog = log.remove();

            try {
                commitLog.log(timedLog);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void log(EntryWithTime entry) {
        log.add(entry);
    }

    public void clear(long timestamp) {
        log.removeIf(e -> e.getTimestamp() < timestamp);
        commitLog.clean();
    }

    public Iterator<EntryWithTime> load() {
        return commitLog.load();
    }
}
