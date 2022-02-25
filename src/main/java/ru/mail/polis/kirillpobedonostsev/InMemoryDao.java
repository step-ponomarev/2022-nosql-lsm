package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableMap<byte[], BaseEntry<byte[]>> map =
            new ConcurrentSkipListMap<>(Arrays::compare);

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return map.values().iterator();
        }
        NavigableMap<byte[], BaseEntry<byte[]>> temp;
        if (from == null) {
            temp = map.headMap(to, false);
        } else if (to == null) {
            temp = map.tailMap(from, true);
        } else {
            temp = map.subMap(from, true, to, false);
        }
        return temp.values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) {
        return key == null ? null : map.get(key);
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        map.put(entry.key(), entry);
    }
}
