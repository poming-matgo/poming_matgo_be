package com.pomingmatgo.gameservice.domain.cluster;

// redirectAddress != null이면 그 노드로 재접속 지시 — 방 이전 프로토콜은 따로 없다 (재접속 = RECONNECT_STATE 경로 재사용)
public record RoutingDecision(String redirectAddress) {

    private static final RoutingDecision LOCAL = new RoutingDecision(null);

    public static RoutingDecision local() {
        return LOCAL;
    }

    public static RoutingDecision redirect(String address) {
        return new RoutingDecision(address);
    }

    public boolean isLocal() {
        return redirectAddress == null;
    }
}
