package com.store.ksr.store;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class RedisWindowStoreTemplate extends RedisTemplate<String,byte[]> {

    public RedisWindowStoreTemplate(@Autowired RedisConnectionFactory connectionFactory) {
        setKeySerializer(RedisSerializer.string());
        setHashKeySerializer(RedisSerializer.java());
        setHashValueSerializer(RedisSerializer.java());
        setConnectionFactory(connectionFactory);
        setEnableTransactionSupport(true);
    }
}
