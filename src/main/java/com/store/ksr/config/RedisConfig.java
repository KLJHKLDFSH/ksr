package com.store.ksr.config;

import org.apache.kafka.common.utils.Bytes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class RedisConfig {

    @Bean
    public LettuceConnectionFactory lettuceConnectionFactory(){
        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
        redisClusterConfiguration
                .clusterNode(new RedisNode("localhost",17000))
                .clusterNode(new RedisNode("localhost",17001))
                .clusterNode(new RedisNode("localhost",17002))
                .clusterNode(new RedisNode("localhost",17003));
        LettuceConnectionFactory factory = new LettuceConnectionFactory(redisClusterConfiguration);
        factory.setDatabase(1);
        return factory;
    }
}
