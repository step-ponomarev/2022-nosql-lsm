package ru.mail.polis.stepanponomarev.log;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MappedIterator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public class CommitLog {
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

    public synchronized Iterator<TimestampEntry> load() {
        final long offset = MemoryAccess.getLong(logMemorySegment);

        if (offset == START_OFFSET) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(logMemorySegment.asSlice(START_OFFSET, offset));
    }

    public synchronized void log(TimestampEntry entry) throws IOException {
        final long offset = MemoryAccess.getLong(logMemorySegment);
        final long expectedOffset  = offset + Utils.sizeOf(entry) + Long.BYTES * 2;
        if (expectedOffset >= logMemorySegment.byteSize()) {
            logMemorySegment = createSegment((long) (expectedOffset * SIZE_BUFFER_FACTOR));
        }

        MemoryAccess.setLong(logMemorySegment, Utils.flush(entry, logMemorySegment, offset));
    }

    public synchronized void clean() throws IOException {
        MemoryAccess.setLong(logMemorySegment, START_OFFSET);
        logMemorySegment = createSegment((long) (initSize * SIZE_BUFFER_FACTOR));
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
