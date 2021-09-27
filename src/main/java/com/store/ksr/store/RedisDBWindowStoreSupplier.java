package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class RedisDBWindowStoreSupplier implements WindowBytesStoreSupplier {


    private final String name;
    private final long windowSize;
    private final long retentionPeriod;
    private final RedisStoreTemplate redisStoreTemplate;

    public RedisDBWindowStoreSupplier(String name, RedisStoreTemplate redisStoreTemplate,
                                      long windowSize, long retentionPeriod) {
        this.name = name;
        this.redisStoreTemplate = redisStoreTemplate;
        this.windowSize = windowSize;
        this.retentionPeriod = retentionPeriod;
    }

    @Deprecated
    @Override
    public int segments() {
        throw new IllegalStateException("Segments is deprecated and should not be called");
    }

    /**
     * redis window store is not *really* segmented, so just say size is 1 ms
     */
    @Override
    public long segmentIntervalMs() {
        return 1;
    }

    @Override
    public long windowSize() {
        return this.windowSize;
    }

    @Override
    public boolean retainDuplicates() {
        return false;
    }

    @Override
    public long retentionPeriod() {
        return this.retentionPeriod;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        return null;
    }

    @Override
    public String metricsScope() {
        return "redis-window";
    }
}
