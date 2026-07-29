package com.pomingmatgo.gameservice.gamelog;

import org.junit.jupiter.api.DisplayName;
import org.springframework.boot.test.context.SpringBootTest;

// cross-room writer 경로에서도 blind replay 동등성이 성립하는지 — 부모 테스트 전체를 상속 실행
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude="
                + "org.redisson.spring.starter.RedissonAutoConfigurationV2,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration",
        "game.log.store=in-memory",
        "game.log.batch.cross-room=true"})
@DisplayName("커맨드 로그(cross-room writer): 영속화된 로그만으로 완주 게임이 재구성된다")
class GameLogReplayCrossRoomTest extends GameLogReplayTest {
}
