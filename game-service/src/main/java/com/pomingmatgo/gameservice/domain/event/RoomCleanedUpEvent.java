package com.pomingmatgo.gameservice.domain.event;

// RoomCleanupService → AutoPlayScheduler 직접 의존이 만드는 DI cycle을 끊기 위한 우회로
public record RoomCleanedUpEvent(long roomId) {
}
