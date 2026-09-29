package com.pomingmatgo.gameservice.application.room;

import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLock;
import com.pomingmatgo.gameservice.infrastructure.lock.RoomLockManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RoomReadyService {
    private final RoomService rooms;
    private final PreGameService preGame;
    private final RoomLockManager roomLock;

    // 준비 선택과 동일하게 게임 락 → 방 락 순서로 획득한다. 안내는 두 락 밖에서 보낸다.
    @GameLock
    public Mono<Boolean> readyAndPrepare(long roomId, Player player, boolean ready) {
        return roomLock.withLock(roomId,
                rooms.readyFresh(roomId, player, ready)
                        .flatMap(state -> ready && state.allPlayersReady()
                                ? rooms.startGame(state)
                                        .flatMap(started -> Mono.defer(() -> preGame.pickFiveCardsAndSave(roomId)))
                                        .thenReturn(true)
                                : Mono.just(false)),
                () -> new WebSocketBusinessException(WebSocketErrorCode.TOO_MANY_REQUESTS));
    }
}
