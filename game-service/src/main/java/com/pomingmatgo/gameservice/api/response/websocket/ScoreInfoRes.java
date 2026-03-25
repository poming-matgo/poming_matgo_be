package com.pomingmatgo.gameservice.api.response.websocket;

import com.pomingmatgo.gameservice.api.response.websocket.dto.PlayerScoreDto;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

@Getter
@AllArgsConstructor
public class ScoreInfoRes {
    private List<PlayerScoreDto> scores;

    public static ScoreInfoRes from(GameState gameState) {
        List<PlayerScoreDto> scores = Arrays.stream(Player.values())
                .map(player -> new PlayerScoreDto(
                        player.getNumber(),
                        gameState.getPlayerState(player).getScore()
                ))
                .toList();
        return new ScoreInfoRes(scores);
    }
}