package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneSkipIterator<T, E extends Entry<T>> implements Iterator<E> {
    private final Iterator<E> wrappedIterator;
    private E current;

    public TombstoneSkipIterator(Iterator<E> wrappedIterator) {
        this.wrappedIterator = wrappedIterator;
        this.current = getNext(wrappedIterator);
    }

    @Override
    public boolean hasNext() {
        return current != null || wrappedIterator.hasNext();
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No such element");
        }

        final E next = current;
        current = getNext(wrappedIterator);

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
