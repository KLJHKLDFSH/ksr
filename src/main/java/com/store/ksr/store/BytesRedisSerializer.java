package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class BytesRedisSerializer implements RedisSerializer<Bytes> {
    @Override
    public byte[] serialize(Bytes bytes) throws SerializationException {
        return bytes.get();
    }

    @Override
    public Bytes deserialize(byte[] bytes) throws SerializationException {
        return new Bytes(bytes);
    }
}
