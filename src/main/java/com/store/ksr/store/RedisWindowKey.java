package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;

public class RedisWindowKey {
    private final Long timeKey;
    private final transient Bytes objKey;
    private final byte[] serKey;

    public RedisWindowKey(Long timeKey, Bytes objKey) {
        this.timeKey = timeKey;
        this.objKey = objKey;
        this.serKey = objKey.get();
    }

    public Long timeKey() {
        return timeKey;
    }

    public Bytes objKey() {
        return Bytes.wrap(serKey);
    }
}
