package ru.mail.polis.stepanponomarev.store;

import ru.mail.polis.Entry;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class MergeIterator<T, E extends Entry<T>> implements Iterator<E> {
    private final Iterator<E> oldDataIterator;
    private final Iterator<E> newDataIterator;
    private final Comparator<T> comparator;

    private E firstEntry;
    private E secondEntry;

    private MergeIterator(final Iterator<E> left, final Iterator<E> right, Comparator<T> comparator) {
        this.oldDataIterator = left;
        this.newDataIterator = right;
        this.comparator = comparator;

        this.firstEntry = getElement(oldDataIterator);
        this.secondEntry = getElement(newDataIterator);
    }

    public static <T, E extends Entry<T>> Iterator<E> of(List<Iterator<E>> iterators, Comparator<T> comparator) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        final int size = iterators.size();
        if (size == 1) {
            return iterators.get(0);
        }

        return new MergeIterator<>(
                of(iterators.subList(0, size / 2), comparator),
                of(iterators.subList(size / 2, size), comparator),
                comparator
        );
    }

    @Override
    public boolean hasNext() {
        return firstEntry != null || secondEntry != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No such element");
        }

        final int compareResult = compare(firstEntry, secondEntry);

        if (compareResult == 0) {
            final E next = secondEntry;
            firstEntry = getElement(oldDataIterator);
            secondEntry = getElement(newDataIterator);

            return next;
        }

        if (compareResult < 0) {
            final E next = firstEntry;
            firstEntry = getElement(oldDataIterator);

            return next;
        }

        final E next = secondEntry;
        secondEntry = getElement(newDataIterator);

        return next;
    }

    private int compare(E r1, E r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return comparator.compare(r1.key(), r2.key());
    }

    private E getElement(final Iterator<E> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
