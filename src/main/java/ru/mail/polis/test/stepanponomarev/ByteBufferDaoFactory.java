package ru.mail.polis.test.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.InMemoryDao;
import ru.mail.polis.test.DaoFactory;


import java.nio.charset.StandardCharsets;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<byte[], Entry<byte[]>> {

    @Override
    public Dao<byte[], Entry<byte[]>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(byte[] data) {
        if (data == null) {
            return null;
        }

        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] fromString(String data) {
        if (data == null) {
            return null;
        }

        return data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Entry<byte[]> fromBaseEntry(Entry<byte[]> entry) {
        return new BaseEntry<>(entry.key(), entry.value());
    }
}
