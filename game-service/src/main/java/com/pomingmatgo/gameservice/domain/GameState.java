package com.pomingmatgo.gameservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.*;
import org.springframework.data.annotation.Id;

import java.io.Serializable;
import java.util.Objects;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.Player.PLAYER_2;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class GameState implements Serializable {
    private static final int MIN_GO_STOP_SCORE = 7;
    private static final long serialVersionUID = 1L;
    @Id
    private Long roomId;

    //player 1
    private Long player1Id;
    private boolean player1Ready;
    private int player1Score;
    private int player1Go;
    private int player1GoScore;

    //player 2
    private Long player2Id;
    private boolean player2Ready;
    private int player2Score;
    private int player2Go;
    private int player2GoScore;

    //game phase
    private boolean gameStarted;
    private int leadingPlayer;
    private int currentTurn;
    private int round;

    @Builder.Default
    private GamePhase phase = GamePhase.IN_PROGRESS;
    private ChoiceInfo choiceInfo; // phase가 await류일때만 의미 있다.

    public Player getPlayerType(long userId) {
        if (Objects.equals(this.player1Id, userId)) {
            return Player.PLAYER_1;
        }
        if (Objects.equals(this.player2Id, userId)) {
            return Player.PLAYER_2;
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

    public static GameState createEmptyRoom(Long roomId) {
        return GameState.builder()
                .roomId(roomId)
                .phase(GamePhase.NONE)
                .build();
    }

    @JsonIgnore
    public Player getCurrentPlayer() {
        return this.getLeadingPlayer() == this.getCurrentTurn() ? PLAYER_1 : PLAYER_2;
    }

    @JsonIgnore
    public Player getOtherPlayer() {
        return this.getLeadingPlayer() == this.getCurrentTurn() ? PLAYER_2 : PLAYER_1;
    }

    public boolean canGoStop(Player player) {
        int score = player.getNumber() == 1 ? player1Score : player2Score;
        int prevGoScore = player.getNumber() == 1 ? player1GoScore : player2GoScore;
        return score >= MIN_GO_STOP_SCORE && score > prevGoScore;
    }

    @JsonIgnore
    public boolean isPlaying() {
        return this.phase == GamePhase.IN_PROGRESS;
    }
}