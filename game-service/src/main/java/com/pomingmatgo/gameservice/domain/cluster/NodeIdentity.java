package com.pomingmatgo.gameservice.domain.cluster;

import org.springframework.stereotype.Component;

import java.util.UUID;

// 프로세스 고유 정체성 — lease owner_instance와 node_registry instance_id가 같은 값이어야 소유자 조회 라우팅이 성립한다.
// 재기동은 항상 새 정체성이다 — 이전 프로세스의 lease/멤버십을 물려받지 않는다 (인수는 복구 절차로)
@Component
public class NodeIdentity {

    private final String id = UUID.randomUUID().toString();

    public String id() {
        return id;
    }
}
