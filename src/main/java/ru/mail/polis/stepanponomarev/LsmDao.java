package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final SSTable ssTable;
    private final SortedMap<ByteBuffer, Entry<ByteBuffer>> memTable;

    public LsmDao(Path path) throws IOException {
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

        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        ssTable.flush(memTable.values().iterator());
    }

    private SortedMap<ByteBuffer, Entry<ByteBuffer>> createMemTable() throws IOException {
        final SortedMap<ByteBuffer, Entry<ByteBuffer>> data = new ConcurrentSkipListMap<>();

        Iterator<Entry<ByteBuffer>> entryIterator = ssTable.get();
        while (entryIterator.hasNext()) {
            Entry<ByteBuffer> entry = entryIterator.next();

            data.put(entry.key(), entry);
        }

        return data;
    }
}
