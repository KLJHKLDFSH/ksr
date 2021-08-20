package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class RedisDBWindowStoreSupplier implements WindowBytesStoreSupplier {


    private final String name;
    private final long windowSize;
    private final long segmentIntervalMs;
    private final boolean retainDuplicates;
    private final long retentionPeriod;
    private final RedisStoreTemplate redisStoreTemplate;

    public RedisDBWindowStoreSupplier(String name, RedisStoreTemplate redisStoreTemplate, long windowSize, long segmentIntervalMs, boolean retainDuplicates, long retentionPeriod) {
        this.name = name;
        this.redisStoreTemplate = redisStoreTemplate;
        this.windowSize = windowSize;
        this.segmentIntervalMs = segmentIntervalMs;
        this.retainDuplicates = retainDuplicates;
        this.retentionPeriod = retentionPeriod;
    }

    @Deprecated
    @Override
    public int segments() {
        throw new IllegalStateException("Segments is deprecated and should not be called");
    }

    @Override
    public long segmentIntervalMs() {
        return this.segmentIntervalMs;
    }

    @Override
    public long windowSize() {
        return this.windowSize;
    }

    @Override
    public boolean retainDuplicates() {
        return this.retainDuplicates;
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
        return new RedisDBWindowedStore(this.name,redisStoreTemplate,windowSize);
    }

    @Override
    public String metricsScope() {
        return null;
    }
}
