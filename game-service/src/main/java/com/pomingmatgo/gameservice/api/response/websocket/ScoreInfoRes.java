package com.pomingmatgo.gameservice.api.response.websocket;

import com.pomingmatgo.gameservice.api.response.websocket.dto.PlayerScoreDto;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.stream.Stream;

@Getter
@AllArgsConstructor
public class ScoreInfoRes {
    private List<PlayerScoreDto> scores;

    public static ScoreInfoRes from(GameState gameState) {
        // PLAYER_NOTHING은 실제 플레이어가 아니므로 제외 (getPlayerState가 player2를 돌려줘 가짜 0번 엔트리가 생겼었음)
        List<PlayerScoreDto> scores = Stream.of(Player.PLAYER_1, Player.PLAYER_2)
                .map(player -> new PlayerScoreDto(
                        player.getNumber(),
                        gameState.getPlayerState(player).getScore()
                ))
                .toList();
        return new ScoreInfoRes(scores);
    }
}