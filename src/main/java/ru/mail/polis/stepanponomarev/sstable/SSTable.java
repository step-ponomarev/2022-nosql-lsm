package ru.mail.polis.stepanponomarev.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

public final class SSTable implements Closeable {
    public static final long TOMBSTONE_TAG = -1;
    private static final String SSTABLE_FILE_NAME = "sstable.data";
    private static final String INDEX_FILE_NAME = "sstable.index";

    private static final String TIMESTAMP_DELIM = "_T_";
    private static final String SSTABLE_DIR_PREFIX = "SSTABLE_DIR";

    private final Path path;
    private final long createdTimeMs;

    private final MemorySegment indexMemorySegment;
    private final MemorySegment tableMemorySegment;

    private SSTable(
            Path path,
            MemorySegment indexMemorySegment,
            MemorySegment tableMemorySegment,
            long createdAt) {
        this.path = path;
        this.indexMemorySegment = indexMemorySegment;
        this.tableMemorySegment = tableMemorySegment;
        this.createdTimeMs = createdAt;
    }

    public static class DataWithInfo {
        final Iterator<TimestampEntry> data;
        final long sizeBytes;
        final int count;

        public DataWithInfo(Iterator<TimestampEntry> data, long sizeBytes, int count) {
            this.data = data;
            this.sizeBytes = sizeBytes;
            this.count = count;
        }

        public Iterator<TimestampEntry> getData() {
            return data;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }

        public int getCount() {
            return count;
        }
    }

    public static SSTable createInstance(Path workingDir, DataWithInfo dataWithInfo) throws IOException {
        if (Files.notExists(workingDir)) {
            throw new IllegalArgumentException("Dir is not exists");
        }

        final long timestamp = System.currentTimeMillis();

        final Path sstableDir = workingDir.resolve(SSTABLE_DIR_PREFIX + createHash(timestamp));
        Files.createDirectory(sstableDir);

        final Path sstableFile = sstableDir.resolve(SSTABLE_FILE_NAME);
        Files.createFile(sstableFile);

        final long sstableSizeBytes = (long) Long.BYTES * 2 * dataWithInfo.getCount() + dataWithInfo.getSizeBytes();
        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                sstableFile,
                0,
                sstableSizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        final Path indexFile = sstableDir.resolve(INDEX_FILE_NAME);
        Files.createFile(indexFile);

        final long indexSizeBytes = (long) Long.BYTES * dataWithInfo.getCount();
        final MemorySegment mappedIndex = MemorySegment.mapFile(
                indexFile,
                0,
                indexSizeBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        flush(dataWithInfo.getData(), mappedSsTable, mappedIndex);

        return new SSTable(sstableDir, mappedIndex.asReadOnly(), mappedSsTable.asReadOnly(), timestamp);
    }

    public static List<SSTable> wakeUpSSTables(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists");
        }

        try (Stream<Path> files = Files.list(path)) {
            final List<String> tableDirNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_PREFIX))
                    .sorted()
                    .toList();

            final List<SSTable> tables = new CopyOnWriteArrayList<>();
            for (String name : tableDirNames) {
                final String[] split = name.split(TIMESTAMP_DELIM);
                if (split.length != 3) {
                    throw new IllegalStateException("Invalid SSTable dir name");
                }
                
                
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }

    private static SSTable upInstance(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Dir is not exists");
        }

        final Path sstableFile = path.resolve(SSTABLE_FILE_NAME);
        final Path indexFile = path.resolve(INDEX_FILE_NAME);
        if (Files.notExists(path) || Files.notExists(indexFile)) {
            throw new IllegalArgumentException("Files must exist.");
        }

        final MemorySegment mappedSsTable = MemorySegment.mapFile(
                sstableFile,
                0,
                Files.size(sstableFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        final MemorySegment mappedIndex = MemorySegment.mapFile(
                indexFile,
                0,
                Files.size(indexFile),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new SSTable(path, mappedIndex, mappedSsTable, System.currentTimeMillis());
    }

    @Override
    public void close() {
        indexMemorySegment.scope().close();
        tableMemorySegment.scope().close();
    }

    private static void flush(Iterator<TimestampEntry> data, MemorySegment sstable, MemorySegment index) {
        long indexOffset = 0;
        long sstableOffset = 0;
        while (data.hasNext()) {
            MemoryAccess.setLongAtOffset(index, indexOffset, sstableOffset);
            indexOffset += Long.BYTES;

            final TimestampEntry entry = data.next();
            sstableOffset += flush(entry, sstable, sstableOffset);
        }
    }

    public void remove() throws IOException {
        this.close();
        removeFilesWithNested(Collections.singletonList(this.path));
    }

    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) {
        final long size = tableMemorySegment.byteSize();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return new MappedIterator(tableMemorySegment);
        }

        final int max = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;
        final int fromIndex = from == null ? 0 : Math.abs(findIndexOfKey(from));

        if (fromIndex > max) {
            return Collections.emptyIterator();
        }

        final int toIndex = to == null ? max + 1 : Math.abs(findIndexOfKey(to));
        final long fromPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, fromIndex);
        final long toPosition = toIndex > max ? size : MemoryAccess.getLongAtIndex(indexMemorySegment, toIndex);

        return new MappedIterator(tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }

    private int findIndexOfKey(MemorySegment key) {
        int low = 0;
        int high = (int) (indexMemorySegment.byteSize() / Long.BYTES) - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid);
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);
            final MemorySegment current = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);

            final int compareResult = Utils.compare(current, key);
            if (compareResult < 0) {
                low = mid + 1;
            } else if (compareResult > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -low;
    }

    private static long flush(TimestampEntry entry, MemorySegment memorySegment, long offset) {
        final MemorySegment key = entry.key();
        final long keySize = key.byteSize();

        long writeOffset = offset;
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, keySize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, keySize).copyFrom(key);
        writeOffset += keySize;

        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, entry.getTimestamp());
        writeOffset += Long.BYTES;

        final MemorySegment value = entry.value();
        if (value == null) {
            MemoryAccess.setLongAtOffset(memorySegment, writeOffset, TOMBSTONE_TAG);
            return writeOffset + Long.BYTES - offset;
        }

        final long valueSize = value.byteSize();
        MemoryAccess.setLongAtOffset(memorySegment, writeOffset, valueSize);
        writeOffset += Long.BYTES;

        memorySegment.asSlice(writeOffset, valueSize).copyFrom(value);
        writeOffset += valueSize;

        return writeOffset - offset;
    }

    private static String createHash(long timestamp) {
        final int HASH_SIZE = 40;

        StringBuilder hash = new StringBuilder(createTimeMark(timestamp))
                .append("_H_")
                .append(System.nanoTime());

        while (hash.length() < HASH_SIZE) {
            hash.append(0);
        }

        return hash.substring(0, HASH_SIZE);
    }

    private static void removeFilesWithNested(Collection<Path> files) throws IOException {
        for (Path dirs : files) {
            try (Stream<Path> ssTableFiles = Files.walk(dirs)) {
                final Iterator<Path> filesToRemove = ssTableFiles.sorted(Comparator.reverseOrder()).iterator();
                while (filesToRemove.hasNext()) {
                    Files.delete(filesToRemove.next());
                }
            }
        }
    }

    private static String createTimeMark(long timestamp) {
        return TIMESTAMP_DELIM + timestamp + TIMESTAMP_DELIM;
    }

    public long getCreatedTime() {
        return createdTimeMs;
    }
}
