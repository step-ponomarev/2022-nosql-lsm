package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, Entry<OSXMemorySegment>> memTable;
    private SizeCounter sizeCounter;

    private static class SizeCounter {
        private transient LongAdder adder;
        private static final VarHandle ADDER;

        static {
            try {
                ADDER = MethodHandles.lookup().findVarHandle(SizeCounter.class, "adder", LongAdder.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public long size() {
            LongAdder a;
            long c;
            do {
            } while ((a = adder) == null &&
                    !ADDER.compareAndSet(this, null, a = new LongAdder()));
            return ((c = a.sum()) <= 0L) ? 0L : c; // ignore transient negatives
        }

        private void addSize(long c) {
            LongAdder a;
            do {
            } while ((a = adder) == null &&
                    !ADDER.compareAndSet(this, null, a = new LongAdder()));
            a.add(c);
        }
    }

    public MemTable() {
        sizeCounter = new SizeCounter();
        memTable = new ConcurrentSkipListMap<>();
    }

    public Entry<OSXMemorySegment> put(OSXMemorySegment key, Entry<OSXMemorySegment> value) {
        final Entry<OSXMemorySegment> oldElement = memTable.get(key);
        final Entry<OSXMemorySegment> entry = memTable.put(key, value);

        final long addedSize = key.size()
                - (oldElement == null ? 0 : oldElement.value().size())
                + (value == null ? 0 : value.value().size());
        sizeCounter.addSize(addedSize);

        return entry;
    }

    public Iterator<Entry<OSXMemorySegment>> get() {
        return get(null, null);
    }

    public long sizeBytes() {
        return this.sizeCounter.size();
    }

    public int size() {
        return this.memTable.size();
    }

    public void clear() {
        memTable.clear();
        sizeCounter = new SizeCounter();
    }

    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) {
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
}
