package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<byte[], Entry<byte[]>> {
    private final NavigableMap<byte[], byte[]> store = new ConcurrentSkipListMap<>(Arrays::compare);

    private static class LazyMemoryAllocationIterator implements Iterator<Entry<byte[]>> {
        private final Iterator<Map.Entry<byte[], byte[]>> iterator;

        public LazyMemoryAllocationIterator(Iterator<Map.Entry<byte[], byte[]>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<byte[]> next() {
            Map.Entry<byte[], byte[]> next = iterator.next();

            return new BaseEntry<>(next.getKey(), next.getValue());
        }
    }

    @Override
    public Iterator<Entry<byte[]>> get(byte[] from, byte[] to) {
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
    public void upsert(Entry<byte[]> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry can't be null");
        }

        byte[] key = entry.key();
        if (entry.value() == null) {
            store.remove(key);
        } else {
            store.put(entry.key(), entry.value());
        }
    }
}
