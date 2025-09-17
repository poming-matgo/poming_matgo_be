package com.pomingmatgo.gameservice.global.config;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.leadingplayer.ChooseLeadPlayer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@EnableAutoConfiguration(exclude={RedisAutoConfiguration.class, RedisReactiveAutoConfiguration.class})
public class RedisConfig {
    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Bean
    public ReactiveRedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory(host, port);
    }

    @Bean(name="gameStateRedisTemplate")
    public ReactiveRedisOperations<String, GameState> gameStateRedisTemplate(ReactiveRedisConnectionFactory redisConnectionFactory) {
        Jackson2JsonRedisSerializer<GameState> serializer = new Jackson2JsonRedisSerializer<>(GameState.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, GameState> builder = RedisSerializationContext
                .newSerializationContext(new StringRedisSerializer());

        RedisSerializationContext<String, GameState> context = builder.value(serializer).hashValue(serializer)
                .hashKey(serializer).build();

        return new ReactiveRedisTemplate<>(redisConnectionFactory, context);
    }

    @Bean(name="cardRedisTemplate")
    public ReactiveRedisOperations<String, String> cardRedisTemplate(ReactiveRedisConnectionFactory redisConnectionFactory) {
        Jackson2JsonRedisSerializer<String> serializer = new Jackson2JsonRedisSerializer<>(String.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, String> builder = RedisSerializationContext
                .newSerializationContext(new StringRedisSerializer());

        RedisSerializationContext<String, String> context = builder.value(serializer).hashValue(serializer)
                .hashKey(serializer).build();

        return new ReactiveRedisTemplate<>(redisConnectionFactory, context);
    }

    @Bean(name="chooseLeadPlayerTemplate")
    public ReactiveRedisOperations<String, ChooseLeadPlayer> chooseLeadPlayerRedisTemplate(ReactiveRedisConnectionFactory redisConnectionFactory) {
        Jackson2JsonRedisSerializer<ChooseLeadPlayer> serializer = new Jackson2JsonRedisSerializer<>(ChooseLeadPlayer.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, ChooseLeadPlayer> builder = RedisSerializationContext
                .newSerializationContext(new StringRedisSerializer());

        RedisSerializationContext<String, ChooseLeadPlayer> context = builder.value(serializer).hashValue(serializer)
                .hashKey(serializer).build();

        return new ReactiveRedisTemplate<>(redisConnectionFactory, context);
    }


}