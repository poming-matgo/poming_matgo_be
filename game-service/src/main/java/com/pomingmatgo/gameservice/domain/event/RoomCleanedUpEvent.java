package com.pomingmatgo.gameservice.domain.event;

/**
 * 방 데이터 정리 시 발행되는 이벤트.
 *
 * RoomCleanupService가 발행하고 AutoPlayScheduler가 수신해 예약된 자동플레이 타이머를 취소한다.
 * 직접 의존(RoomCleanupService → AutoPlayScheduler) 대신 이벤트를 쓰는 이유는
 * AutoPlayScheduler → TurnFlowService → ... → RoomCleanupService bean DI cycle을 끊기 위함.
 */
public record RoomCleanedUpEvent(long roomId) {
}
