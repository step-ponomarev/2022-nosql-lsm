package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneSkipIterator<T, E extends Entry<T>> implements Iterator<E> {
    private final Iterator<E> delegate;
    private E currentElem;

    public TombstoneSkipIterator(Iterator<E> delegate) {
        this.delegate = delegate;
        this.currentElem = getNext(delegate);
    }

    @Override
    public boolean hasNext() {
        return currentElem != null || delegate.hasNext();
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No such element");
        }

        final E next = currentElem;
        currentElem = getNext(delegate);

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
