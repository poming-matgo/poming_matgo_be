package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import lombok.AllArgsConstructor;
import lombok.Getter;

// winner가 PLAYER_NOTHING이면 무승부
@Getter
@AllArgsConstructor
public class GameOverRes {
    private final Player winner;
    private final int score;
    private final int payoutScore;
    private final int goCount;
    private final boolean goBak;

    public static GameOverRes from(GameState finalState, Player winner) {
        if (winner == Player.PLAYER_NOTHING) {
            return new GameOverRes(winner, 0, 0, 0, false);
        }
        return new GameOverRes(
                winner,
                finalState.getPlayerState(winner).getScore(),
                finalState.payoutScoreOf(winner),
                finalState.getPlayerState(winner).getGo(),
                finalState.isGoBak(winner)
        );
    }
}
