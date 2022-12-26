package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

public record BaseEntry<Data>(Data key, Data value) implements Entry<Data> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + "}";
    }
}
