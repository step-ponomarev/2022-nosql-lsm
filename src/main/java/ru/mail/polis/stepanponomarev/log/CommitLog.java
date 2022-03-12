package ru.mail.polis.stepanponomarev.log;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.EntryWithTime;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;
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

    private MemorySegment logMemorySegment;

    public CommitLog(Path path, long size) throws IOException {
        commitLogFile = path.resolve(FILE_NAME);
        final boolean newFile = Files.notExists(commitLogFile);
        if (newFile) {
            Files.createFile(commitLogFile);
        }

        logMemorySegment = createSegment((long) (size * SIZE_BUFFER_FACTOR));
        if (newFile) {
            MemoryAccess.setLong(logMemorySegment, START_OFFSET);
        }
    }

    public synchronized Iterator<EntryWithTime> load() {
        final long offset = MemoryAccess.getLong(logMemorySegment);

        if (offset == START_OFFSET) {
            return Collections.emptyIterator();
        }

        return new MappedIterator(logMemorySegment.asSlice(START_OFFSET, offset));
    }

    public synchronized void log(EntryWithTime entry) throws IOException {
        final long offset = MemoryAccess.getLong(logMemorySegment);
        if (offset >= logMemorySegment.byteSize() / 1.2) {
            logMemorySegment = createSegment((long) (offset * SIZE_BUFFER_FACTOR));
        }

        MemoryAccess.setLong(logMemorySegment, Utils.flush(entry, logMemorySegment, offset));
    }

    public synchronized void clean() {
        MemoryAccess.setLong(logMemorySegment, START_OFFSET);
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
