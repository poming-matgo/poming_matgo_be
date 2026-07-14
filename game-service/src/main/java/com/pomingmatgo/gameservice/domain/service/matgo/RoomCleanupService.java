package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RoomCleanupService {

    private final GameStateRepository gameStateRepository;
    private final InstalledCardRepository installedCardRepository;
    private final AcquiredCardRepository acquiredCardRepository;
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final RoomLockManager roomLockManager;
    private final GameLockCleaner gameLockCleaner;
    private final ApplicationEventPublisher eventPublisher;

    public Mono<Void> cleanupRoomData(long roomId) {
        return Mono.when(
                gameStateRepository.cleanup(roomId),
                installedCardRepository.cleanup(roomId),
                acquiredCardRepository.cleanup(roomId),
                leadingPlayerRepository.cleanup(roomId),
                roomLockManager.cleanup(roomId),
                gameLockCleaner.cleanup(roomId),
                // 예약된 자동플레이 task dispose + scheduled 맵 entry 제거.
                // AutoPlayScheduler 직접 의존은 DI cycle을 만들므로 이벤트로 위임 —
                // 리스너는 발행 스레드에서 동기 실행되어 기존 직접 호출과 동일한 시점에 취소된다
                Mono.fromRunnable(() -> eventPublisher.publishEvent(new RoomCleanedUpEvent(roomId)))
        );
    }
}
