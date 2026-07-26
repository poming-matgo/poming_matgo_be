package com.pomingmatgo.gameservice.global.lock;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface InFlightManager {
    // NORMAL/AUTOPLAY 키 분리가 자동플레이 양보/차단 판정의 전제다
    static String normalKey(long roomId, int playerNum) {
        return "IN_FLIGHT:NORMAL:ROOM:" + roomId + ":PLAYER:" + playerNum;
    }

    static String autoplayKey(long roomId, int playerNum) {
        return "IN_FLIGHT:AUTOPLAY:ROOM:" + roomId + ":PLAYER:" + playerNum;
    }

    /** token은 요청별 소유 토큰 — TTL 만료 후 재획득이 일어나도 뒤늦은 정리가 남의 플래그를 지우지 못하게 한다 */
    Mono<Boolean> trySetFlag(String key, String token, Duration ttl);

    Mono<Boolean> isSet(String key);

    /** 토큰이 일치할 때만 삭제 */
    Mono<Void> deleteFlag(String key, String token);
}
