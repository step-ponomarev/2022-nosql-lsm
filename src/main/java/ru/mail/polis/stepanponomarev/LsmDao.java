package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final SSTable ssTable;
    private final SortedMap<ByteBuffer, Entry<ByteBuffer>> memTable = new ConcurrentSkipListMap<>();

    public LsmDao(Path path) {
        ssTable = new SSTable(path);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        Iterator<Entry<ByteBuffer>> entryIterator = ssTable.get(from, to);

        if (from == null && to == null) {
            return MergeIterator.instanceOf(List.of(entryIterator, memTable.values().iterator()));
        }

        if (from == null) {
            return MergeIterator.instanceOf(List.of(entryIterator, memTable.headMap(to).values().iterator()));
        }

        if (to == null) {
            return MergeIterator.instanceOf(List.of(entryIterator, memTable.tailMap(from).values().iterator()));
        }

        return MergeIterator.instanceOf(List.of(entryIterator, memTable.subMap(from, to).values().iterator()));
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        ssTable.flush(memTable.values().iterator());
    }
}
