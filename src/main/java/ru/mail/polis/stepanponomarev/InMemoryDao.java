package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final NavigableMap<ByteBuffer, ByteBuffer> store = new ConcurrentSkipListMap<>(Comparator.naturalOrder());

    private static class LazyMemoryAllocationIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator;

        private LazyMemoryAllocationIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            Map.Entry<ByteBuffer, ByteBuffer> entry = iterator.next();

            return new BaseEntry<>(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return all();
        }

        if (from == null) {
            return allTo(to);
        }

        if (to == null) {
            return allFrom(from);
        }

        return new LazyMemoryAllocationIterator(
                store.subMap(from, true, to, false).entrySet().iterator()
        );
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        ByteBuffer value = store.get(key);

        return new BaseEntry<>(key, value);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allFrom(ByteBuffer from) {
        if (from == null) {
            return all();
        }

        return new LazyMemoryAllocationIterator(
                store.tailMap(from, true).entrySet().iterator()
        );
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allTo(ByteBuffer to) {
        if (to == null) {
            return all();
        }

        return new LazyMemoryAllocationIterator(
                store.headMap(to, false).entrySet().iterator()
        );
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        store.put(entry.key(), entry.value());
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> all() {
        return new LazyMemoryAllocationIterator(
                store.entrySet().iterator()
        );
    }
}
