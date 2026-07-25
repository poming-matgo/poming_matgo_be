package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.score.Payout;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class ScoreInfoRes {
    private List<PlayerScoreDto> scores;

    // PLAYER_NOTHING은 실제 플레이어가 아니므로 제외 (getPlayerState가 player2를 돌려줘 가짜 0번 엔트리가 생겼었음)
    public static ScoreInfoRes from(GameState gameState, Payout player1Payout, Payout player2Payout) {
        return new ScoreInfoRes(List.of(
                dtoOf(gameState, Player.PLAYER_1, player1Payout),
                dtoOf(gameState, Player.PLAYER_2, player2Payout)));
    }

    private static PlayerScoreDto dtoOf(GameState gameState, Player player, Payout payout) {
        var playerState = gameState.getPlayerState(player);
        return new PlayerScoreDto(player.getNumber(), playerState.getScore(), playerState.getGo(), payout.total());
    }
}
