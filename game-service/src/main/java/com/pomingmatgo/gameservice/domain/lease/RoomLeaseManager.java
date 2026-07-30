package com.pomingmatgo.gameservice.domain.lease;

import com.pomingmatgo.gameservice.domain.event.LeaseLostEvent;
import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import com.pomingmatgo.gameservice.global.config.RoomLeaseProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

// 배타성의 권위는 DB lease + fencing token이다 — 이 캐시는 쓰기 가드에 실을 토큰을 기억할 뿐, 소유권을 판정하지 않는다.
// 토큰 캐시는 인스턴스 로컬(sticky routing 전제) — SessionManager identity guard의 프로세스 경계판
@Component
@RequiredArgsConstructor
@Slf4j
public class RoomLeaseManager {

    private final RoomLeaseRepository leaseRepository;
    private final RoomLeaseProperties properties;
    private final ApplicationEventPublisher eventPublisher;
    private final String instanceId = UUID.randomUUID().toString();
    private final ConcurrentHashMap<Long, Long> tokens = new ConcurrentHashMap<>();
    private Disposable heartbeat;

    @PostConstruct
    void startHeartbeat() {
        if (!leaseRepository.enabled()) {
            return;
        }
        heartbeat = Flux.interval(properties.heartbeatInterval())
                // DB 지연으로 밀린 tick은 버린다 — interval은 backpressure를 못 받아 밀리면 overflow로 루프째 죽는다
                .onBackpressureDrop()
                .concatMap(tick -> leaseRepository.heartbeat(instanceId, properties.duration())
                        // heartbeat 실패는 곧 lease 만료 → 인수 대상이 될 뿐 — 쓰기 배타성은 fencing이 지키므로 재시도만 한다
                        .onErrorResume(e -> {
                            log.warn("lease heartbeat 실패 — instanceId={}", instanceId, e);
                            return Mono.empty();
                        }))
                .subscribe();
        log.info("room lease 활성 — instanceId={}, duration={}, heartbeatInterval={}",
                instanceId, properties.duration(), properties.heartbeatInterval());
    }

    @PreDestroy
    void stopHeartbeat() {
        if (heartbeat != null) {
            heartbeat.dispose();
        }
    }

    /** false면 fencing 가드 전체가 무비용 통과 — 기존 SQL·수치가 그대로 재현돼야 한다(직교성) */
    public boolean fencingEnabled() {
        return leaseRepository.enabled();
    }

    /** 게임 시작 전 소유권 확보 — 실패는 다른 인스턴스의 유효 lease 보유이므로 게임 시작을 중단한다(fail-fast) */
    public Mono<Void> acquire(long roomId) {
        if (!leaseRepository.enabled()) {
            return Mono.empty();
        }
        return leaseRepository.acquire(roomId, instanceId, properties.duration())
                .doOnNext(token -> tokens.put(roomId, token))
                .switchIfEmpty(Mono.error(() ->
                        new IllegalStateException("방 lease 획득 실패 — 다른 인스턴스가 소유 중, roomId=" + roomId)))
                .then();
    }

    /** 쓰기 가드에 실을 토큰. null = 이 인스턴스 소유가 아님 (fencing 활성 시 해당 방 쓰기는 폐기 대상) */
    public Long tokenOf(long roomId) {
        return tokens.get(roomId);
    }

    /** 잔여 쓰기(markCompleted 등)가 끝난 뒤 구독돼야 한다 — 해제 즉시 이 방의 쓰기는 fencing에 막힌다 */
    public Mono<Void> release(long roomId) {
        // defer 필수: cleanup 체인의 .then() 인자로 조립 시점에 평가된다 — 여기서 바로 회수하면
        // 아직 drain 중인 마지막 배치와 markCompleted가 자기 토큰 없이 막힌다 (AFK 검증에서 실제로 잡힌 버그)
        return Mono.defer(() -> {
            Long token = tokens.remove(roomId);
            if (token == null) {
                return Mono.empty();
            }
            return leaseRepository.release(roomId, token);
        });
    }

    /** 인수 후보 스캔(2-D) — 만료됐지만 정상 해제는 아닌 방 */
    public Flux<Long> findExpiredRooms() {
        return leaseRepository.findExpiredRoomIds();
    }

    /**
     * 만료 lease 원자적 인수 — 성공 시 새 token을 캐시에 채워 이후 쓰기(로그·완료 표시)가 이 인스턴스 소유로 통과한다.
     * owner를 자기 instanceId로 넣으므로 heartbeat가 인수한 방도 함께 연장한다
     */
    public Mono<RoomLeaseRepository.Takeover> takeover(long roomId) {
        return leaseRepository.takeover(roomId, instanceId, properties.duration())
                .doOnNext(result -> tokens.put(roomId, result.fencingToken()));
    }

    /** 복구 실패 시 — 즉시 만료로 되돌려 다음 스캔(이 노드든 타 노드든)이 재시도하게 한다 */
    public Mono<Void> abandon(long roomId) {
        // release와 같은 이유로 defer 필수 — 부수효과(캐시 회수)는 구독 시점에
        return Mono.defer(() -> {
            Long token = tokens.remove(roomId);
            if (token == null) {
                return Mono.empty();
            }
            return leaseRepository.abandon(roomId, token);
        });
    }

    /** fencing 거부 후 호출 — DB 토큰과 대조해 상실이 확인된 방만 캐시 회수 + LeaseLostEvent 발행 */
    public Mono<Void> verifyOwnership(Collection<Long> roomIds) {
        return Flux.fromIterable(roomIds)
                .flatMap(roomId -> {
                    Long cached = tokens.get(roomId);
                    if (cached == null) {
                        return Mono.empty();
                    }
                    return leaseRepository.currentToken(roomId)
                            // 행이 없으면 소유가 성립할 수 없다 — 상실로 판정
                            .defaultIfEmpty(-1L)
                            .filter(current -> !current.equals(cached))
                            .doOnNext(current -> {
                                tokens.remove(roomId);
                                log.error("방 소유권 상실 — roomId={}, cachedToken={}, dbToken={}", roomId, cached, current);
                                eventPublisher.publishEvent(new LeaseLostEvent(roomId));
                            });
                })
                .then();
    }
}
