package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable{
    private static final String fileName = "ss.table";
    private static final String indexFile = "index.file";

    private final Path path;
    private final SortedMap<ComparableMemorySegmentWrapper, Long> memIndex;

    public SSTable(Path basePath) throws IOException {
        path = basePath;

        //TODO: Читать с диска
        memIndex = new TreeMap<>();
        if (!path.toFile().exists()) {
            return;
        }
    }

    public void flush(Iterator<Entry<ComparableMemorySegmentWrapper>> data) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(getFilePath(fileName), FileChannelUtils.APPEND_OPEN_OPTIONS)) {
            while (data.hasNext()) {
                final long keyPos = fileChannel.position();
                final Entry<ComparableMemorySegmentWrapper> entry = data.next();

                ByteBuffer key = entry.key().getMemorySegment().asByteBuffer();
                int keySize = key.remaining();

                fileChannel.write(Utils.toByteBuffer(keySize));
                fileChannel.write(key);

                ByteBuffer value = entry.value() == null ? null : entry.value().getMemorySegment().asByteBuffer();
                int valueSize = value == null ? Utils.TOMBSTONE_TAG : value.remaining();

                fileChannel.write(Utils.toByteBuffer(valueSize));
                if (value == null) {
                    continue;
                }
                fileChannel.write(value);

                memIndex.put(entry.key(), keyPos);
            }
        }

        // Flush memIndex
    }

    public Entry<ComparableMemorySegmentWrapper> get(ComparableMemorySegmentWrapper key) throws IOException {
        SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> data = get();
        Iterator<Entry<ComparableMemorySegmentWrapper>> iterator = data.tailMap(key).values().iterator();
        if (!iterator.hasNext()) {
            return null;
        }

        Entry<ComparableMemorySegmentWrapper> next = iterator.next();
        if (next.key().equals(key)) {
            return next;
        }

        return null;
    }

    public Iterator<Entry<ComparableMemorySegmentWrapper>> get(ComparableMemorySegmentWrapper from, ComparableMemorySegmentWrapper to) throws IOException {
        SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> data = get();
        if (from == null && to == null) {
            return data.values().iterator();
        }

        if (from == null) {
            return data.headMap(to).values().iterator();
        }

        if (to == null) {
            return data.tailMap(from).values().iterator();
        }

        return data.subMap(from, to).values().iterator();
    }

    private Path getFilePath(String fileName) {
        return new File(path.toAbsolutePath() + "/" + fileName).toPath();
    }

    private SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> get() throws IOException {
        final Path filePath = getFilePath(fileName);

        if (!filePath.toFile().exists()) {
            return Collections.emptySortedMap();
        }

        final SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> data = new TreeMap<>();

        try (final FileChannel fileChannel = FileChannel.open(filePath, FileChannelUtils.READ_OPEN_OPTIONS)) {
            final MemorySegment memorySegment = MemorySegment.mapFile(filePath, 0, fileChannel.size(), FileChannel.MapMode.READ_ONLY, ResourceScope.newSharedScope());
            final long size = memorySegment.byteSize();

            long position = 0;
            while (position != size) {
                int keySize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                ComparableMemorySegmentWrapper key = new ComparableMemorySegmentWrapper(memorySegment.asSlice(position, keySize));
                position += keySize;

                int valueSize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                if (valueSize == Utils.TOMBSTONE_TAG) {
                    data.remove(key);
                    continue;
                }

                ComparableMemorySegmentWrapper value = new ComparableMemorySegmentWrapper(memorySegment.asSlice(position, valueSize));
                position += valueSize;

                data.put(key, new BaseEntry<>(
                        key,
                        value
                ));
            }
        }

        return data;
    }
}
