package com.pomingmatgo.gameservice.global.lock;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface InFlightManager {
    /**
     * @param token 요청별 고유 소유 토큰. deleteFlag에서 소유자 검증에 사용된다.
     *              TTL 만료 후 다른 요청이 플래그를 재획득한 경우, 뒤늦게 끝난 원 소유자의
     *              정리 호출이 새 소유자의 플래그를 지우지 못하도록 한다.
     */
    Mono<Boolean> trySetFlag(String key, String token, Duration ttl);

    Mono<Boolean> isSet(String key);

    /** 현재 플래그의 토큰이 일치할 때만 삭제한다 (소유자 검증). */
    Mono<Void> deleteFlag(String key, String token);
}
