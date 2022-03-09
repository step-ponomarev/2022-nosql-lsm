package ru.mail.polis.stepanponomarev.sstable;

import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

final class Utils {
    public static final int TOMBSTONE_TAG = -1;

    public static final EnumSet<StandardOpenOption> APPEND_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
    );

    public static final EnumSet<StandardOpenOption> READ_OPEN_OPTIONS = EnumSet.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ
    );

    private Utils() {
    }
}
