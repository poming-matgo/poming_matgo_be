package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.event.RoomCleanedUpEvent;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.lease.RoomLeaseManager;
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
    private final GameCommandLog gameCommandLog;
    private final RoomLeaseManager roomLeaseManager;
    private final ApplicationEventPublisher eventPublisher;

    public Mono<Void> cleanupRoomData(long roomId) {
        return Mono.when(
                gameStateRepository.cleanup(roomId),
                installedCardRepository.cleanup(roomId),
                acquiredCardRepository.cleanup(roomId),
                leadingPlayerRepository.cleanup(roomId),
                roomLockManager.cleanup(roomId),
                gameLockCleaner.cleanup(roomId),
                // 로그는 delete가 아니라 drain + 완료 표시 — 커맨드 로그는 방 정리 후에도 저장소에 남는다.
                // lease 해제는 완료 표시(fencing 가드 쓰기)까지 끝난 뒤여야 한다 — 순서를 바꾸면 자기 마지막 쓰기가 자기에게 막힌다
                gameCommandLog.close(roomId).then(roomLeaseManager.release(roomId)),
                // 자동플레이 타이머 취소 — AutoPlayScheduler 직접 의존은 DI cycle을 만들어 이벤트로 위임한다.
                // 리스너는 발행 스레드에서 동기 실행되므로 취소 시점은 직접 호출과 같다
                Mono.fromRunnable(() -> eventPublisher.publishEvent(new RoomCleanedUpEvent(roomId)))
        );
    }
}
