package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTableProxy {
    private final MemTable tableToWrite;
    private final FlushData flushData;

    public static final class FlushData {
        public final Iterator<TimestampEntry> data;
        public final long sizeBytes;
        public final int count;

        public FlushData(Iterator<TimestampEntry> flushData, long sizeBytes, int count) {
            this.data = flushData;
            this.sizeBytes = sizeBytes;
            this.count = count;
        }
    }

    public MemTableProxy(MemTable memTable) {
        this.tableToWrite = memTable;
        this.flushData = null;
    }

    private MemTableProxy(MemTable tableToWrite, FlushData flushData) {
        this.tableToWrite = tableToWrite;
        this.flushData = flushData;
    }

    public static MemTableProxy createPreparedToFlush(MemTableProxy memTableWizard) {
        final SortedMap<OSXMemorySegment, TimestampEntry> clone = new ConcurrentSkipListMap<>();

        long sizeBytes = 0;
        final Iterator<TimestampEntry> timestampEntryIterator = memTableWizard.tableToWrite.get();
        while (timestampEntryIterator.hasNext()) {
            TimestampEntry entry = timestampEntryIterator.next();

            sizeBytes += Utils.sizeOf(entry);
            clone.put(entry.key(), entry);
        }

        final FlushData flushData = new FlushData(
                clone.values().iterator(),
                sizeBytes,
                clone.size()
        );

        return new MemTableProxy(memTableWizard.tableToWrite, flushData);
    }

    public static MemTableProxy createFlushNullable(MemTableProxy memTableWizard) {
        return new MemTableProxy(memTableWizard.tableToWrite, null);
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        return tableToWrite.get(from, to);
    }

    public TimestampEntry put(TimestampEntry entry) {
        return tableToWrite.put(entry);
    }

    public FlushData getFlushData() {
        return flushData;
    }
}
