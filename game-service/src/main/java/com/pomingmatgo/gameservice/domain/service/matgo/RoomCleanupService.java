package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import com.pomingmatgo.gameservice.global.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
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
    private final SessionManager sessionManager;

    public Mono<Void> cleanupRoom(long roomId) {
        // 데이터 정리가 오류로 끝나도 세션 정리를 시도하고, 두 단계의 오류를 모두 보존한다.
        return Flux.concatDelayError(
                Mono.defer(() -> cleanupRoomData(roomId)),
                Mono.defer(() -> sessionManager.removeRoom(roomId))
        ).then().doOnError(error -> log.error("Room ({}) cleanup failed", roomId, error));
    }

    public Mono<Void> cleanupRoomData(long roomId) {
        // 개별 오류가 다른 정리를 취소하지 않게 하고, 동기 예외도 구독 시 오류로 합산한다.
        return Mono.whenDelayError(
                // 데이터 삭제 전에 신규 타이머를 차단한다. 동기 리스너로 등록과 종료를 직렬화한다.
                Mono.fromRunnable(() -> eventPublisher.publishEvent(new RoomCleanedUpEvent(roomId))),
                Mono.defer(() -> gameStateRepository.cleanup(roomId)),
                Mono.defer(() -> installedCardRepository.cleanup(roomId)),
                Mono.defer(() -> acquiredCardRepository.cleanup(roomId)),
                Mono.defer(() -> leadingPlayerRepository.cleanup(roomId)),
                Mono.defer(() -> roomLockManager.cleanup(roomId)),
                Mono.defer(() -> gameLockCleaner.cleanup(roomId))
        ).doOnError(error -> log.error("Room ({}) data cleanup failed", roomId, error));
    }
}
