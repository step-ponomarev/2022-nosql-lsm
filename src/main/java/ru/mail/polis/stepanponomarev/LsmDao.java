package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final Logger logger;
    private final SSTable ssTable;
    private final SortedMap<ByteBuffer, Entry<ByteBuffer>> memTable;

    public LsmDao(Path path) throws IOException {
        logger = new Logger(path);
        ssTable = new SSTable(path);
        memTable = createMemTable();
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        if (from == null && to == null) {
            return memTable.values().iterator();
        }

        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }

        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }

        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        try {
            memTable.put(entry.key(), entry);
            logger.append(entry.key(), entry.value());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    @Override
    public void flush() throws IOException {
        ssTable.flush(memTable.values().iterator());
        logger.clear();
        memTable.clear();
    }

    private SortedMap<ByteBuffer, Entry<ByteBuffer>> createMemTable() throws IOException {
        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new ConcurrentSkipListMap<>();

        Iterator<Entry<ByteBuffer>> mergedDataIterator = MergeIterator.instanceOf(List.of(ssTable.get(), logger.get()));
        while (mergedDataIterator.hasNext()) {
            Entry<ByteBuffer> entry = mergedDataIterator.next();

            data.put(entry.key(), entry);
        }

        return data;
    }

    private Entry<ByteBuffer> convert(Map.Entry<ByteBuffer, ByteBuffer> data) {
        ByteBuffer key = data.getKey();
        ByteBuffer value = data.getValue();

        return new BaseEntry<>(key, value);
    }
}
