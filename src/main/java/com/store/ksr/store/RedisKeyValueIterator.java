package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

public class RedisKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private Iterator<KeyValue<Bytes, byte[]>> iterator;
    public RedisKeyValueIterator(Map<Bytes, byte[]> map) {
        LinkedList<KeyValue<Bytes, byte[]>> list  = new LinkedList<>();
        map.forEach( (key,value) -> list.add(new KeyValue<Bytes, byte[]>(key,value)) );
        this.iterator = list.iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public Bytes peekNextKey() {
        return iterator.next().key;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        return iterator.next();
    }
}
