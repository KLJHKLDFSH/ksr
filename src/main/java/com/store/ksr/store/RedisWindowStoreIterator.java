package com.store.ksr.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class RedisWindowStoreIterator implements WindowStoreIterator<byte[]> {


    @Override
    public void close() {

    }

    @Override
    public Long peekNextKey() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public KeyValue<Long, byte[]> next() {
        return null;
    }
}
