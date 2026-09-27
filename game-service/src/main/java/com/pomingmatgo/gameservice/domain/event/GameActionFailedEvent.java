package com.pomingmatgo.gameservice.domain.event;

// gate 해제 전에 동기 전달해 실패한 방의 정리를 예약한다.
public record GameActionFailedEvent(long roomId) {
}
