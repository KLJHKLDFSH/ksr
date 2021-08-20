package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class RedisDBWindowedStore
        implements WindowStore<Bytes, byte[]> {

    private final String name;
    private final RedisStoreTemplate redisStoreTemplate;
    private final long windowSize;
    public RedisDBWindowedStore(String name,
                                RedisStoreTemplate redisStoreTemplate,
                                long windowSize) {
        this.name = name;
        this.redisStoreTemplate = redisStoreTemplate;
        this.windowSize = windowSize;
    }
    @Deprecated
    @Override
    public void put(Bytes key, byte[] value) {
    }

    @Override
    public void put(Bytes key, byte[] value, long windowStartTimestamp) {
        redisStoreTemplate.opsForHash().put(this.name, key, value);
    }

    @Override
    public byte[] fetch(Bytes key, long time) {
        return new byte[0];
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
        return null;
    }


    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return false;
    }
}
