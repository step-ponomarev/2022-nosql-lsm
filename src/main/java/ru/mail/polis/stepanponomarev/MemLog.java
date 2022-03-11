package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class MemLog {
    private static final String FILE_NAME = "sstable.log";
    private static final long START_OFFSET = Long.BYTES;
    private static final double BUFFER_DELTA = 1.5;

    private final Path logFile;

    private MemorySegment logMemorySegment;
    private final AtomicLong writeOffset;

    public MemLog(Path path, long size) throws IOException {
        logFile = path.resolve(FILE_NAME);
        final boolean fileNotExists = Files.notExists(logFile);
        if (fileNotExists) {
            Files.createFile(logFile);
        }

        logMemorySegment = createSegment((long) (size * BUFFER_DELTA));
        writeOffset = fileNotExists
                ? new AtomicLong(START_OFFSET)
                : new AtomicLong(MemoryAccess.getLong(logMemorySegment));
    }

    public Iterator<Entry<OSXMemorySegment>> load() {
        if (writeOffset.get() == START_OFFSET) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(logMemorySegment.asSlice(START_OFFSET, writeOffset.get()));
    }

    public void log(Entry<OSXMemorySegment> entry) throws IOException {
        synchronized (this) {
            if (writeOffset.get() >= logMemorySegment.byteSize() / 1.2) {
                logMemorySegment = createSegment((long) (writeOffset.get() * BUFFER_DELTA));
            }
        }

        writeOffset.set(Utils.flush(entry, logMemorySegment, writeOffset.get()));
        MemoryAccess.setLong(logMemorySegment, writeOffset.get());
    }

    public void clean() {
        writeOffset.set(START_OFFSET);
        MemoryAccess.setLong(logMemorySegment, START_OFFSET);
    }

    private MemorySegment createSegment(long size) throws IOException {
        return MemorySegment.mapFile(
                logFile,
                0,
                size,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );
    }
}
