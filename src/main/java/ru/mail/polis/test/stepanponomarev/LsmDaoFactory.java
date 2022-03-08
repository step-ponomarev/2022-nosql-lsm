package ru.mail.polis.test.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.LsmDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class LsmDaoFactory implements DaoFactory.Factory<ByteBuffer, Entry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao(Config config) throws IOException {
        return new LsmDao(config.basePath());
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) {
            return null;
        }

        if (data.array().length == 0) {
            throw new IllegalArgumentException("Buffer should have array");
        }

        return new String(data.array(), StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        if (data == null) {
            return null;
        }

        return ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> entry) {
        return new BaseEntry<>(entry.key(), entry.value());
    }
}
