package com.pomingmatgo.gameservice.domain.event;

/** fencing 거부로 방 소유권 상실이 확인됨 — 이 인스턴스의 로컬 사본은 더 이상 진실이 아니다 */
public record LeaseLostEvent(long roomId) {
}
