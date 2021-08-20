package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisDBKeyValueStore implements KeyValueStore<Bytes,byte[]> {

    private static final Logger log = LoggerFactory.getLogger(RedisDBKeyValueStore.class);

    private final String name;
    private final Bytes key;

    private final boolean isOpen;

    private final RedisStoreTemplate redisStoreTemplate;

    public RedisDBKeyValueStore(String name , RedisStoreTemplate redisStoreTemplate) {
        this.name = name;
        this.key = new Bytes(name.getBytes());
        this.isOpen = true;
        this.redisStoreTemplate = redisStoreTemplate;

    }
    /** =========================== tateStore =========================== **/
    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        //connect redis
        RedisDBStoreCallback redisDBStoreCallback = new RedisDBStoreCallback(this.name, redisStoreTemplate);
        context.register(root, redisDBStoreCallback);
    }

    @Override
    public void flush() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() {
        log.info("redis store close");
    }

    /** =========================== KeyValueStore =========================== **/
    @Override
    public void put(Bytes key, byte[] value) {
        validKeyAndValue(key, value);
        redisStoreTemplate.<Bytes,byte[]>opsForHash().put(this.name,key,value);
    }

    @Override
    public byte[]  putIfAbsent(Bytes key, byte[] value) {
        validKeyAndValue(key, value);
        boolean flag = redisStoreTemplate.<Bytes,byte[]>opsForHash().putIfAbsent(this.name,key,value);
        if (flag){
            return value;
        }
        return null;
    }

    @Override
    public void putAll(List<KeyValue<Bytes , byte[] >> entries) {
        Map<Bytes,byte[]> map = new HashMap<>();
        entries.forEach(kv -> map.put(kv.key, kv.value));
        redisStoreTemplate.opsForHash().putAll(this.name, map);
    }

    @Override
    public byte[] delete(Bytes key) {
        byte[] v = redisStoreTemplate.execute(new SessionCallback<byte[]>() {
            @Override
            public <K,V> byte[] execute(RedisOperations<K, V> operations) throws DataAccessException {
                byte[] value = operations.<Bytes,byte[]>opsForHash().get((K) name,  key);
                operations.<Bytes, byte[]>opsForHash().delete((K) name,key);
                return value;
            }
        });
        byte[] value = redisStoreTemplate.<Bytes,byte[]>opsForHash().get(this.name, key);
        long count = redisStoreTemplate.<Bytes,byte[]>opsForHash().delete(name,key);
        if(value != null && count == 1){
            return value;
        }
        return null;
    }
    /** =========================== ReadOnlyKeyValueStore =========================== **/
    @Override
    public byte[] get(Bytes key) {
        return redisStoreTemplate.<Bytes,byte[]>opsForHash().get(this.name, key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        Map<Bytes,byte[]> values = redisStoreTemplate.<Bytes,byte[]>opsForHash().entries(this.name);
        return new RedisKeyValueIterator(values);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long approximateNumEntries() {
        return redisStoreTemplate.opsForHash().size(this.name);
    }

    private void validKeyAndValue(Bytes key,byte[] value){
        Assert.notNull(key,"key must not be null");
        Assert.notNull(value,"key must not be null");
    }

    /** =========================== inner class =========================== **/
    static class RedisDBStoreCallback implements StateRestoreCallback {

        private final String name;
        private final RedisStoreTemplate redisStoreTemplate;

        public RedisDBStoreCallback(String name, RedisStoreTemplate redisStoreTemplate) {
            this.name = name;
            this.redisStoreTemplate = redisStoreTemplate;
        }

        @Override
        public void restore(byte[] key, byte[] value) {
            redisStoreTemplate.opsForHash().put(this.name,new Bytes(key), value);
        }
    }


}
