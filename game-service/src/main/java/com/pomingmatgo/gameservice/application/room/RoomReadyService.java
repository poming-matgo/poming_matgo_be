package com.pomingmatgo.gameservice.application.room;

import com.pomingmatgo.gameservice.application.pregame.PreGameService;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.infrastructure.lock.GameLock;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RoomReadyService {
    private final RoomService rooms;
    private final PreGameService preGame;

    // 준비 저장과 시작 phase 전이를 같은 게임 락 안에서 완료하고 안내는 락 밖에서 보낸다.
    @GameLock
    public Mono<Boolean> readyAndPrepare(long roomId, Player player, boolean ready) {
        return rooms.readyFresh(roomId, player, ready)
                .flatMap(state -> ready && state.allPlayersReady()
                        ? rooms.startGame(state)
                                .flatMap(started -> Mono.defer(() -> preGame.pickFiveCardsAndSave(roomId)))
                                .thenReturn(true)
                        : Mono.just(false));
    }
}
