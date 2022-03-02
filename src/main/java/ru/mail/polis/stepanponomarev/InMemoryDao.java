package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final SortedMap<ByteBuffer, ByteBuffer> store = new ConcurrentSkipListMap<>();

    private static class LazyMemoryAllocationIterator implements Iterator<Entry<ByteBuffer>> {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator;

        public LazyMemoryAllocationIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<ByteBuffer> next() {
            Map.Entry<ByteBuffer, ByteBuffer> next = iterator.next();

            return new BaseEntry<>(next.getKey(), next.getValue());
        }
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return new LazyMemoryAllocationIterator(store.entrySet().iterator());
        }

        if (from == null) {
            return new LazyMemoryAllocationIterator(store.headMap(to).entrySet().iterator());
        }

        if (to == null) {
            return new LazyMemoryAllocationIterator(store.tailMap(from).entrySet().iterator());
        }

        return new LazyMemoryAllocationIterator(store.subMap(from, to).entrySet().iterator());
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry can't be null");
        }

        ByteBuffer key = entry.key();
        if (entry.value() == null) {
            store.remove(key);
        } else {
            store.put(entry.key(), entry.value());
        }
    }
}
