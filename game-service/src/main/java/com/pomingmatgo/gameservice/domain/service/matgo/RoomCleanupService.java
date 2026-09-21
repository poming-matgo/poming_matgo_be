package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Log4j2
public class RoomCleanupService {

    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final RoomLockManager roomLockManager;
    private final GameLockCleaner gameLockCleaner;
    private final ApplicationEventPublisher eventPublisher;

    public Mono<Void> cleanupRoomData(long roomId) {
        // 개별 오류가 다른 정리를 취소하지 않게 하고, 동기 예외도 구독 시 오류로 합산한다.
        return Mono.whenDelayError(
                Mono.defer(() -> gameStateRepository.cleanup(roomId)),
                Mono.defer(() -> installedCardRepository.cleanup(roomId)),
                Mono.defer(() -> acquiredCardRepository.cleanup(roomId)),
                Mono.defer(() -> leadingPlayerRepository.cleanup(roomId)),
                Mono.defer(() -> roomLockManager.cleanup(roomId)),
                Mono.defer(() -> gameLockCleaner.cleanup(roomId)),
                // 자동플레이 타이머 취소 — AutoPlayScheduler 직접 의존은 DI cycle을 만들어 이벤트로 위임한다.
                // 리스너는 발행 스레드에서 동기 실행되므로 취소 시점은 직접 호출과 같다
                Mono.fromRunnable(() -> eventPublisher.publishEvent(new RoomCleanedUpEvent(roomId)))
        ).doOnError(error -> log.error("Room ({}) data cleanup failed", roomId, error));
    }
}
