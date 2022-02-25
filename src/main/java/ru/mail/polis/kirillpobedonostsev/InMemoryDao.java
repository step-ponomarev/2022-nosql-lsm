package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableSet<BaseEntry<byte[]>> set =
            new ConcurrentSkipListSet<>((o1, o2) -> Arrays.compare(o1.key(), o2.key()));

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return set.iterator();
        }
        NavigableSet<BaseEntry<byte[]>> temp;
        if (from != null && to != null) {
            temp = set.subSet(new BaseEntry<>(from, null), true, new BaseEntry<>(to, null), false);
        } else if (from != null) {
            temp = set.tailSet(new BaseEntry<>(from, null), true);
        } else {
            temp = set.headSet(new BaseEntry<>(to, null), false);
        }
        return temp.iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) {
        BaseEntry<byte[]> entry = set.floor(new BaseEntry<>(key, null));
        return entry == null || !Arrays.equals(entry.key(), key) ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        set.add(entry);
    }
}
