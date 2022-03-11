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

public class MemLog {
    private static final String FILE_NAME = "sstable.log";
    private static final long START_OFFSET = Long.BYTES;
    private static final double BUFFER_DELTA = 1.5;

    private final Path logFile;

    private MemorySegment logMemorySegment;

    public MemLog(Path path, long size) throws IOException {
        logFile = path.resolve(FILE_NAME);
        final boolean newFile = Files.notExists(logFile);
        if (newFile) {
            Files.createFile(logFile);
        }

        logMemorySegment = createSegment((long) (size * BUFFER_DELTA));
        if (newFile) {
            MemoryAccess.setLong(logMemorySegment, START_OFFSET);
        }
    }

    public Iterator<Entry<OSXMemorySegment>> load() {
        final long offset = MemoryAccess.getLong(logMemorySegment);
        if (offset == START_OFFSET) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(logMemorySegment.asSlice(START_OFFSET, offset));
    }

    public void log(Entry<OSXMemorySegment> entry) throws IOException {
        final long offset = MemoryAccess.getLong(logMemorySegment);
        if (offset >= logMemorySegment.byteSize() / 1.2) {
            logMemorySegment = createSegment((long) (offset * BUFFER_DELTA));
        }

        MemoryAccess.setLong(logMemorySegment, Utils.flush(entry, logMemorySegment, offset));
    }

    public void clean() {
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
