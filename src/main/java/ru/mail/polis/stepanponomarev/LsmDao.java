package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.log.AsyncLogger;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class LsmDao implements Dao<OSXMemorySegment, Entry<OSXMemorySegment>> {
    private static final long MAX_MEM_TABLE_SIZE_BYTES = 1_000_000;
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final MemTable memTable;
    private final AsyncLogger logger;

    private final Path path;
    private final CopyOnWriteArrayList<SSTable> store;

    public LsmDao(Path bathPath) throws IOException {
        if (Files.notExists(bathPath)) {
            Files.createDirectory(bathPath);
        }

        path = bathPath;
        logger = new AsyncLogger(path, MAX_MEM_TABLE_SIZE_BYTES);
        memTable = new MemTable(logger.load());
        store = createStore(path);
    }

    @Override
    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<Entry<OSXMemorySegment>>> iterators = new ArrayList<>();
        for (SSTable table : store) {
            iterators.add(table.get(from, to));
        }

        iterators.add(memTable.get(from, to));

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(Entry<OSXMemorySegment> entry) {
        final long timestamp = System.currentTimeMillis();

        try {
            logger.log(entry, timestamp);
            memTable.put(entry);

            if (memTable.sizeBytes() >= MAX_MEM_TABLE_SIZE_BYTES) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.currentTimeMillis();

        final MemTable snapshot = memTable.getSnapshotAndClean();
        final Path dir = path.resolve(SSTABLE_DIR_NAME + store.size());
        Files.createDirectory(dir);

        store.add(SSTable.createInstance(dir, snapshot.get(), snapshot.sizeBytes(), snapshot.size()));
        logger.clear(timestamp);
    }

    private CopyOnWriteArrayList<SSTable> createStore(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            final long ssTableCount = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .count();

            final CopyOnWriteArrayList<SSTable> tables = new CopyOnWriteArrayList<>();
            for (long i = 0; i < ssTableCount; i++) {
                tables.add(SSTable.upInstance(path.resolve(SSTABLE_DIR_NAME + i)));
            }

            return tables;
        }
    }
}
