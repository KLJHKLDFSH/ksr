package com.store.ksr.store;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class RedisStoreTemplate extends RedisTemplate<String,byte[]> {

    public RedisStoreTemplate(@Autowired RedisConnectionFactory connectionFactory) {
        setKeySerializer(RedisSerializer.string());
        setHashKeySerializer(new BytesRedisSerializer());
        setConnectionFactory(connectionFactory);
        setEnableTransactionSupport(true);
    }
}
