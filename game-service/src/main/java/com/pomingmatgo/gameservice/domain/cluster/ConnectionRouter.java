package com.pomingmatgo.gameservice.domain.cluster;

import com.pomingmatgo.gameservice.domain.repository.RoomLeaseRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Optional;

// 접속 시점 소유자 조회 + 리다이렉트 — 게임 중(hot path)엔 홉 0, 라우팅 비용은 로컬에 방이 없는 접속에서만 발생.
// 권위는 lease owner, 링은 미소유 방(게임 전·신규)의 배치 힌트일 뿐 — 링이 뭐라 하든 유효 lease 소유자가 이긴다
@Component
@RequiredArgsConstructor
public class ConnectionRouter {

    private final ClusterRing clusterRing;
    private final RoomLeaseRepository leaseRepository;
    private final NodeIdentity identity;

    /** 로컬에 방이 없을 때만 부를 것 — 로컬 상태 보유 = 이 노드가 서빙 (좀비 판정은 fencing 거부 → LeaseLostEvent 몫) */
    public Mono<RoutingDecision> route(long roomId) {
        if (!clusterRing.enabled()) {
            return Mono.just(RoutingDecision.local());
        }
        return leaseRepository.findActiveOwner(roomId)
                .map(this::toOwner)
                .defaultIfEmpty(planned(roomId));
    }

    private RoutingDecision toOwner(String owner) {
        if (identity.id().equals(owner)) {
            // 자기 소유인데 로컬 상태가 없다 = 복구 진행 중 창 — 로컬 판정으로 흘려 NOT_EXISTED_ROOM, 클라 재시도가 흡수
            return RoutingDecision.local();
        }
        // 소유자가 죽어 주소를 모르면 리다이렉트 불가 — lease 만료 후 인수가 끝나야 갈 곳이 생긴다 (그때까지 클라 재시도)
        return clusterRing.addressOf(owner)
                .map(RoutingDecision::redirect)
                .orElse(RoutingDecision.local());
    }

    private RoutingDecision planned(long roomId) {
        if (clusterRing.isPlannedLocal(roomId)) {
            return RoutingDecision.local();
        }
        Optional<String> address = clusterRing.plannedNode(roomId).flatMap(clusterRing::addressOf);
        return address.map(RoutingDecision::redirect).orElse(RoutingDecision.local());
    }
}
