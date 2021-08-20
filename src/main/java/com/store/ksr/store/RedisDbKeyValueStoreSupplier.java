package com.store.ksr.store;

import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;

public class RedisDbKeyValueStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private RedisStoreTemplate redisStoreTemplate;

    public RedisDbKeyValueStoreSupplier(String name, RedisStoreTemplate redisStoreTemplate) {
        this.name = name;
        this.redisStoreTemplate = redisStoreTemplate;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public RedisDBKeyValueStore get() {
        return new RedisDBKeyValueStore(this.name, redisStoreTemplate);
    }

    @Override
    public String metricsScope() {
        return "redis";
    }


}
