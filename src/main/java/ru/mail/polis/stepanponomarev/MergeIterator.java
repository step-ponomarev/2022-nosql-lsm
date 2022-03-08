package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class MergeIterator implements Iterator<Entry<ComparableMemorySegmentWrapper>> {
    private final Iterator<Entry<ComparableMemorySegmentWrapper>> firstIter;
    private final Iterator<Entry<ComparableMemorySegmentWrapper>> secondIter;

    private Entry<ComparableMemorySegmentWrapper> firstRecord;
    private Entry<ComparableMemorySegmentWrapper> secondRecord;

    private MergeIterator(final Iterator<Entry<ComparableMemorySegmentWrapper>> left, final Iterator<Entry<ComparableMemorySegmentWrapper>> right) {
        firstIter = right;
        secondIter = left;

        this.firstRecord = getElement(firstIter);
        this.secondRecord = getElement(secondIter);
    }

    public static Iterator<Entry<ComparableMemorySegmentWrapper>> instanceOf(List<Iterator<Entry<ComparableMemorySegmentWrapper>>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        var size = iterators.size();
        if (size == 1) {
            return iterators.get(0);
        }

        return merge(
                instanceOf(iterators.subList(0, size / 2)),
                instanceOf(iterators.subList(size / 2, size))
        );
    }

    private static Iterator<Entry<ComparableMemorySegmentWrapper>> merge(Iterator<Entry<ComparableMemorySegmentWrapper>> left, Iterator<Entry<ComparableMemorySegmentWrapper>> right) {
        return new MergeIterator(left, right);
    }

    @Override
    public boolean hasNext() {
        return firstRecord != null || secondRecord != null;
    }

    @Override
    public Entry<ComparableMemorySegmentWrapper> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No Such Element");
        }

        final int compareResult = compare(firstRecord, secondRecord);
        final Entry<ComparableMemorySegmentWrapper> next = compareResult > 0
                ? secondRecord
                : firstRecord;

        if (compareResult < 0) {
            firstRecord = getElement(firstIter);
        }

        if (compareResult > 0) {
            secondRecord = getElement(secondIter);
        }

        if (compareResult == 0) {
            firstRecord = getElement(firstIter);
            secondRecord = getElement(secondIter);
        }

        return next;
    }

    private int compare(Entry<ComparableMemorySegmentWrapper> r1, Entry<ComparableMemorySegmentWrapper> r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return r1.key().compareTo(r2.key());
    }

    private Entry<ComparableMemorySegmentWrapper> getElement(final Iterator<Entry<ComparableMemorySegmentWrapper>> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
