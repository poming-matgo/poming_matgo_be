package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.InstalledCard;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public class InstalledCardRepository {
    @Qualifier("installedCardTemplate")
    @Autowired
    private ReactiveRedisOperations<String, InstalledCard> redisOps;
    private static final String GAME_STATE_KEY_PREFIX = "installedCard:";

    public Mono<Boolean> save(InstalledCard installedCard, long roomId) {
        String redisKey = GAME_STATE_KEY_PREFIX+roomId;
        return redisOps.opsForValue().set(redisKey, installedCard);
    }
}
