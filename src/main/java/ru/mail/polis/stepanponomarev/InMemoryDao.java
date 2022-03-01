package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    final ConcurrentNavigableMap<ByteBuffer, ByteBuffer> store = new ConcurrentSkipListMap<>(Comparator.naturalOrder());

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return all();
        }

        if (from != null) {
            return allFrom(from);
        }

        if (to != null) {
            return allTo(to);
        }

        return store.entrySet()
                .stream()
                .filter(e -> isBetween(e.getKey(), from, to))
                .map(e -> new BaseEntry<>(e.getKey(), e.getValue()))
                .iterator();
    }

    private boolean isBetween(ByteBuffer current, ByteBuffer from, ByteBuffer to) {
        if (from != null && from.compareTo(current) < 0) {
            return false;
        }

        if (to != null && to.compareTo(current) >= 0) {
            return false;
        }

        return true;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        ByteBuffer value = store.get(key);

        return value == null ? null : new BaseEntry<>(key, value);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allFrom(ByteBuffer from) {
        if (from == null) {
            return all();
        }

        return store.entrySet()
                .stream()
                .filter(e -> from.compareTo(e.getKey()) >= 0)
                .map(e -> new BaseEntry<>(e.getKey(), e.getValue())).iterator();
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allTo(ByteBuffer to) {
        if (to == null) {
            return all();
        }

        return store.entrySet()
                .stream()
                .filter(e -> to.compareTo(e.getKey()) < 0)
                .map(e -> new BaseEntry<>(e.getKey(), e.getValue())).iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        store.put(entry.key(), entry.value());
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> all() {
        return store.entrySet().stream()
                .map(e -> new BaseEntry<>(e.getKey(), e.getValue()))
                .iterator();
    }
}
