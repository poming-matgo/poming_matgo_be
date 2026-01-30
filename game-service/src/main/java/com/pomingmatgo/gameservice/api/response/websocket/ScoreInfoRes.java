package com.pomingmatgo.gameservice.api.response.websocket;

import com.pomingmatgo.gameservice.api.response.websocket.dto.PlayerScoreDto;
import com.pomingmatgo.gameservice.domain.GameState;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class ScoreInfoRes {
    private List<PlayerScoreDto> scores;

    public static ScoreInfoRes from(GameState gameState) {
        PlayerScoreDto p1ScoreDto = new PlayerScoreDto(
                1,
                gameState.getPlayer1Score()
        );

        PlayerScoreDto p2ScoreDto = new PlayerScoreDto(
                2,
                gameState.getPlayer2Score()
        );

        return new ScoreInfoRes(List.of(p1ScoreDto, p2ScoreDto));
    }
}