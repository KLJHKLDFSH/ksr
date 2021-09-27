package com.store.ksr.store;

import com.store.ksr.utli.ObjectWithByte;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class MapRedisSerializer implements RedisSerializer<Segment> {

    @Override
    public byte[] serialize(Segment segment) throws SerializationException {
        return ObjectWithByte.toByteArray(segment);
    }

    @Override
    public Segment  deserialize(byte[] bytes) throws SerializationException {
        return (Segment) ObjectWithByte.toObject(bytes);
    }
}
