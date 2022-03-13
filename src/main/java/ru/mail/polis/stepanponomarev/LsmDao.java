package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.log.LoggerAhead;
import ru.mail.polis.stepanponomarev.memtable.MemTable;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class LsmDao implements Dao<OSXMemorySegment, TimestampEntry> {
    private static final Logger log = Logger.getLogger(LsmDao.class.getSimpleName());

    private static final long MAX_MEM_TABLE_SIZE_BYTES = 1;
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final LoggerAhead loggerAhead;

    private final AtomicLong currentSize = new AtomicLong();
    private final CopyOnWriteArrayList<SSTable> ssTables;
    private final CopyOnWriteArrayList<MemTable.FlushData> flushSnapshots;

    private volatile MemTable memTable;

    public LsmDao(Path basePath) throws IOException {
        if (Files.notExists(basePath)) {
            throw new IllegalArgumentException("Path: " + basePath + "is not exist");
        }

        path = basePath;
        loggerAhead = new LoggerAhead(path, MAX_MEM_TABLE_SIZE_BYTES);
        memTable = createMemTable(loggerAhead.load());
        ssTables = createStore(path);
        flushSnapshots = new CopyOnWriteArrayList<>();
    }

    private MemTable createMemTable(Iterator<TimestampEntry> data) {
        final SortedMap<OSXMemorySegment, TimestampEntry> store = new ConcurrentSkipListMap<>();
        while (data.hasNext()) {
            final TimestampEntry entry = data.next();
            store.put(entry.key(), entry);
        }

        return new MemTable(store);
    }

    private CopyOnWriteArrayList<SSTable> createStore(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            final List<String> sstableNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .sorted()
                    .toList();

            final CopyOnWriteArrayList<SSTable> tables = new CopyOnWriteArrayList<>();
            for (String name : sstableNames) {
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }

    @Override
    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<TimestampEntry>> iterators = new ArrayList<>();
        for (SSTable table : ssTables) {
            iterators.add(table.get(from, to));
        }

        for (MemTable.FlushData flushData : flushSnapshots) {
            iterators.add(flushData.get(from, to));
        }

        iterators.add(memTable.get(from, to));

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        memTable.put(entry);
        loggerAhead.log(entry);
        currentSize.addAndGet(Utils.sizeOf(entry));

        try {
            if (currentSize.get() >= MAX_MEM_TABLE_SIZE_BYTES) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        loggerAhead.close();
        flush();
    }

    @Override
    public void flush() throws IOException {
        final MemTable.FlushData flushData;
        synchronized (memTable.getFlushData()) {
            memTable = MemTable.createPreparedToFlush(memTable);
            flushData = memTable.getFlushData();
            flushSnapshots.add(flushData);
            currentSize.set(0);
        }

        if (flushData.count == 0) {
            flushSnapshots.remove(flushData);
            return;
        }

        final Path dir = path.resolve(SSTABLE_DIR_NAME + flushData.timestamp);
        Files.createDirectory(dir);

        ssTables.add(SSTable.createInstance(dir, flushData.get(), flushData.sizeBytes, flushData.count));
        flushSnapshots.remove(flushData);

        synchronized (memTable.getFlushData()) {
            memTable = MemTable.createFlushNullable(memTable);
        }

        loggerAhead.clear(flushData.timestamp);

        log.info("FLUSHED | ENTRY_COUNT: %d | SIZE_IN_BYTES: %d".formatted(flushData.count, flushData.sizeBytes));
    }
}
