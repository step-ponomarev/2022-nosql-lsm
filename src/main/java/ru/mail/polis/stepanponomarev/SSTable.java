package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable implements Closeable {
    private static final int tombstoneTag = -1;
    private static final String fileName = "ss.table";

    private static final OpenOption[] writeOpenOptions = {
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
    };
    private static final OpenOption[] reatOpenOptions = {
            StandardOpenOption.READ
    };


    private final Path path;
    private FileChannel fileChannel;

    public SSTable(Path path) throws IOException {
        this.path = new File(path.toAbsolutePath() + "/" + fileName).toPath();

        if (path.toFile().exists()) {
            this.fileChannel = FileChannel.open(this.path, writeOpenOptions);
        }
    }

    public void flush(Iterator<Entry<ComparableMemorySegmentWrapper>> data) throws IOException {
        while (data.hasNext()) {
            Entry<ComparableMemorySegmentWrapper> next = data.next();
            FileChannelUtils.append(
                    fileChannel,
                    next.key().getMemorySegment().asByteBuffer(),
                    next.value().getMemorySegment().asByteBuffer(),
                    tombstoneTag
            );
        }
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

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    private SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> get() throws IOException {
        if (!path.toFile().exists()) {
            return Collections.emptySortedMap();
        }

        final SortedMap<ComparableMemorySegmentWrapper, Entry<ComparableMemorySegmentWrapper>> data = new TreeMap<>();

        try (final FileChannel fileChannel = FileChannel.open(path, reatOpenOptions)) {
            final MemorySegment memorySegment = MemorySegment.mapFile(path, 0, fileChannel.size(), FileChannel.MapMode.READ_ONLY, ResourceScope.newSharedScope());
            final long size = memorySegment.byteSize();

            long position = 0;
            while (position != size) {
                int keySize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                ComparableMemorySegmentWrapper key = new ComparableMemorySegmentWrapper(memorySegment.asSlice(position, keySize));
                position += keySize;

                int valueSize = memorySegment.asSlice(position, Integer.BYTES).asByteBuffer().getInt();
                position += Integer.BYTES;

                if (valueSize == tombstoneTag) {
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
