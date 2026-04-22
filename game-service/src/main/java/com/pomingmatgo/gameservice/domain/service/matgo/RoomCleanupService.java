package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.lock.GameLockCleaner;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import lombok.RequiredArgsConstructor;
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

    public Mono<Void> cleanupRoomData(long roomId) {
        return Mono.when(
                gameStateRepository.cleanup(roomId),
                installedCardRepository.cleanup(roomId),
                acquiredCardRepository.cleanup(roomId),
                leadingPlayerRepository.cleanup(roomId),
                roomLockManager.cleanup(roomId),
                gameLockCleaner.cleanup(roomId)
        );
    }
}
