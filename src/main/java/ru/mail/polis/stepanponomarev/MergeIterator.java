package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class MergeIterator<T extends Comparable<T>> implements Iterator<Entry<T>> {
    private final Iterator<Entry<T>> firstIter;
    private final Iterator<Entry<T>> secondIter;

    private Entry<T> firstRecord;
    private Entry<T> secondRecord;

    private MergeIterator(final Iterator<Entry<T>> left, final Iterator<Entry<T>> right) {
        firstIter = right;
        secondIter = left;

        firstRecord = getElement(firstIter);
        secondRecord = getElement(secondIter);
    }

    public static <T extends Comparable<T>> Iterator<Entry<T>> instanceOf(List<Iterator<Entry<T>>> iterators) {
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

    private static <T extends Comparable<T>> Iterator<Entry<T>> merge(Iterator<Entry<T>> left, Iterator<Entry<T>> right) {
        return new MergeIterator<>(left, right);
    }

    @Override
    public boolean hasNext() {
        return firstRecord != null || secondRecord != null;
    }

    @Override
    public Entry<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No Such Element");
        }

        final int compareResult = compare(firstRecord, secondRecord);
        final Entry<T> next = compareResult > 0
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

    private int compare(Entry<T> r1, Entry<T> r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return r1.key().compareTo(r2.key());
    }

    private Entry<T> getElement(final Iterator<Entry<T>> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
