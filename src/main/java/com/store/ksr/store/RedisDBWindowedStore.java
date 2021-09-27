package com.store.ksr.store;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;

import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class RedisDBWindowedStore
        implements WindowStore<Bytes, byte[]> {

    private static final Logger log = LoggerFactory.getLogger(RedisDBWindowedStore.class);

    private final String name;
    private RedisWindowStoreTemplate redisWindowStoreTemplate;
    private final long windowSize;
    private ProcessorContext context;
    private final long retentionPeriod;
    private boolean open;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    public RedisDBWindowedStore(String name,
                                RedisWindowStoreTemplate redisWindowStoreTemplate,
                                long windowSize, long retentionPeriod) {
        this.name = name;
        this.redisWindowStoreTemplate = redisWindowStoreTemplate;
        this.windowSize = windowSize;
        this.retentionPeriod = retentionPeriod;
    }
    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;
        RedisWindowStateRestoreCallback callback = new RedisWindowStateRestoreCallback();
        context.register(root, callback);
        open = true;
    }

    @Deprecated
    @Override
    public void put(Bytes key, byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(Bytes key, byte[] value, long windowStartTimestamp) {
        //step 1 remove expired segment
        removeExpiredSegments();
        //step 2 put
        innerPut(key, value ,windowStartTimestamp);
    }

    private void innerPut(Bytes key, byte[] value, long windowStartTimestamp){
        observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);
        if(windowStartTimestamp <= observedStreamTime - retentionPeriod){
            log.warn("Skip record for expired segment");
        }else {
            if (value != null){
                transactionPut(key, value, windowStartTimestamp);
            }
        }
    }

    /**
     * open transaction for put
     * @param key
     * @param value
     * @param windowStartTimestamp
     */
    private void transactionPut(Bytes key, byte[] value, long windowStartTimestamp){
        redisWindowStoreTemplate.execute(new SessionCallback<ConcurrentNavigableMap<BytesWrapper, byte[]>>() {
            @Override
            public <K, V> ConcurrentNavigableMap<BytesWrapper, byte[]> execute(RedisOperations<K, V> operations) throws DataAccessException {
                operations.watch((K) name);
                ConcurrentNavigableMap<BytesWrapper, byte[]> cMap = getForTransaction(windowStartTimestamp, operations);
                if(cMap != null){
                    cMap.put(BytesWrapper.wrap(key), value);
                    operations.<Long, ConcurrentNavigableMap<BytesWrapper, byte[]>>opsForHash().put((K)name, windowStartTimestamp, cMap);
                }else {
                    ConcurrentNavigableMap<BytesWrapper, byte[]> map = new ConcurrentSkipListMap<>();
                    map.put(BytesWrapper.wrap(key), value);
                    operations.<Long, ConcurrentNavigableMap<BytesWrapper, byte[]>>opsForHash().put((K)name, windowStartTimestamp, map);
                }
                return null;
            }
        });
    }

    private ConcurrentNavigableMap<BytesWrapper, byte[]> get(long windowStartTimestamp){
      return redisWindowStoreTemplate.<Long,ConcurrentNavigableMap<BytesWrapper, byte[]>>opsForHash()
                        .get(this.name, windowStartTimestamp);
    }

    private <K,V> ConcurrentNavigableMap<BytesWrapper, byte[]> getForTransaction(long windowStartTimestamp,RedisOperations<K, V> operations ){
        return operations.<Long,ConcurrentNavigableMap<BytesWrapper, byte[]>>opsForHash()
                .get((K)name, windowStartTimestamp);
    }
    private void removeExpiredSegments() {
        long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);
        redisWindowStoreTemplate.opsForHash().delete(this.name, minLiveTime);
    }

    @Override
    public byte[] fetch(Bytes key, long windowStartTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod){
            return null;
        }

        final ConcurrentNavigableMap<BytesWrapper, byte[]> kvMap = get(windowStartTimestamp);
        if(kvMap == null){
            return null;
        }else {
            return kvMap.get(BytesWrapper.wrap(key));
        }
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

    private WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo, final boolean forward){
        Objects.requireNonNull(key, "key cannot be null");
        removeExpiredSegments();
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);
        return null;
    }



    @Override
    public void flush() {

    }

    @Override
    public void close() {
        redisWindowStoreTemplate.delete(this.name);
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    private static class RedisWindowStateRestoreCallback implements StateRestoreCallback {

        @Override
        public void restore(byte[] key, byte[] value) {

        }
    }
}
