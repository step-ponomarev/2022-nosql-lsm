package ru.mail.polis.stepanponomarev.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.logging.Logger;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MappedIterator;

final class CommitLog implements Closeable {
    private static final Logger log = Logger.getLogger(CommitLog.class.getSimpleName());

    private static final String FILE_NAME = "commit.log";
    private static final long START_OFFSET = Long.BYTES;
    private static final double SIZE_BUFFER_FACTOR = 1.5;

    private final Path commitLogFile;
    private final long initSize;

    private MemorySegment logMemorySegment;

    public CommitLog(Path path, long size) throws IOException {
        initSize = size < Long.BYTES ? Long.BYTES : size;

        commitLogFile = path.resolve(FILE_NAME);
        final boolean newFile = Files.notExists(commitLogFile);
        if (newFile) {
            Files.createFile(commitLogFile);
        }

        logMemorySegment = createSegment((long) (initSize * SIZE_BUFFER_FACTOR));
        if (newFile) {
            MemoryAccess.setLong(logMemorySegment, START_OFFSET);
        }
    }

    @Override
    public void close() throws IOException {
        logMemorySegment.scope().close();
    }

    public synchronized Iterator<TimestampEntry> load() {
        final long offset = MemoryAccess.getLong(logMemorySegment);

        if (offset == START_OFFSET) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(logMemorySegment.asSlice(START_OFFSET, offset));
    }

    public synchronized void log(TimestampEntry entry) throws IOException {
        final long offset = MemoryAccess.getLong(logMemorySegment);
        final long requiredOffset = offset + Utils.sizeOf(entry) + Long.BYTES * 2;
        final long currentSizeInBytes = logMemorySegment.byteSize();

        if (requiredOffset >= currentSizeInBytes) {
            final String logMsg = "ALLOCATE_MEMORY | CURRENT_SIZE_IN_BYTES: %d | REQUIRED_OFFSET: %d";
            log.info(logMsg.formatted(currentSizeInBytes, requiredOffset));

            logMemorySegment = createSegment((long) (requiredOffset * SIZE_BUFFER_FACTOR));
        }

        MemoryAccess.setLong(logMemorySegment, Utils.flush(entry, logMemorySegment, offset));
    }

    public synchronized void clean() throws IOException {
        final long currentSizeBytes = logMemorySegment.byteSize();

        MemoryAccess.setLong(logMemorySegment, START_OFFSET);
        logMemorySegment.scope().close();
        logMemorySegment = createSegment((long) (initSize * SIZE_BUFFER_FACTOR));

        log.info("CLEAN | CURRENT_SIZE_IN_BYTES: %d".formatted(currentSizeBytes));
    }

    private MemorySegment createSegment(long size) throws IOException {
        return MemorySegment.mapFile(
                commitLogFile,
                0,
                size,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );
    }
}
