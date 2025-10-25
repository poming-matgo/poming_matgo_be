package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.*;
import org.springframework.data.annotation.Id;

import java.io.Serializable;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_2;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class GameState implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    Long roomId;
    Long player1Id;
    Long player2Id;
    boolean player1Ready;
    boolean player2Ready;
    int leadingPlayer;
    int round;
    int currentTurn;

    @Builder.Default
    private GamePhase phase = GamePhase.IN_PROGRESS;
    private ChoiceInfo choiceInfo; // phase가 await류일때만 의미 있다.



    public GameState(Long roomId) {
        this.roomId = roomId;
    }

    public int getPlayerNumber(long userId) {
        if (userId == this.player1Id) {
            return 1;
        }
        if (userId == this.player2Id) {
            return 2;
        }
        throw new WebSocketBusinessException(WebSocketErrorCode.NOT_IN_ROOM);
    }

    public GameState withPlayerReady(Player player, boolean isReady) {
        GameStateBuilder builder = this.toBuilder();
        if (player == Player.PLAYER_1) {
            builder.player1Ready(isReady);
        } else {
            builder.player2Ready(isReady);
        }
        return builder.build();
    }

    public Player getCurrentPlayer() {
        return this.getLeadingPlayer()==this.getCurrentTurn() ? PLAYER_1 : PLAYER_2;
    }
}
