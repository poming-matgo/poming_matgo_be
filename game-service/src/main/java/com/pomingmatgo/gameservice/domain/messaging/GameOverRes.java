package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.score.Multiplier;
import com.pomingmatgo.gameservice.domain.score.Payout;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

// winner가 PLAYER_NOTHING이면 무승부
@Getter
@AllArgsConstructor
public class GameOverRes {
    private final Player winner;
    private final int score;
    private final int baseScore;
    private final int payoutScore;
    private final int goCount;
    private final List<Payout.Applied> multipliers;
    private final boolean goBak;

    public static GameOverRes from(GameState finalState, Player winner, Payout payout) {
        if (winner == Player.PLAYER_NOTHING) {
            return new GameOverRes(winner, 0, 0, 0, 0, List.of(), false);
        }
        PlayerState winnerState = finalState.getPlayerState(winner);
        return new GameOverRes(
                winner,
                winnerState.winningScore(),
                payout.baseScore(),
                payout.total(),
                winnerState.getGo(),
                payout.multipliers(),
                payout.has(Multiplier.GO_BAK)
        );
    }
}
