package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneSkipIterator<T, E extends Entry<T>> implements Iterator<E> {
    private final Iterator<E> iteratorWithTombstones;
    private E currentElem;

    public TombstoneSkipIterator(Iterator<E> iteratorWithTombstones) {
        this.iteratorWithTombstones = iteratorWithTombstones;
        this.currentElem = getNext(iteratorWithTombstones);
    }

    @Override
    public boolean hasNext() {
        return currentElem != null || iteratorWithTombstones.hasNext();
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No Such Element");
        }

        final E next = currentElem;
        currentElem = getNext(iteratorWithTombstones);

        return next;
    }

    private E getNext(Iterator<E> iterator) {
        while (iterator.hasNext()) {
            final E entry = iterator.next();
            if (entry.value() != null) {
                return entry;
            }
        }

        return null;
    }
}
