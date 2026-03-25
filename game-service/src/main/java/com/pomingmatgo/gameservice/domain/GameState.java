package com.pomingmatgo.gameservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.*;
import org.springframework.data.annotation.Id;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.UnaryOperator;

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

    @Builder.Default
    private PlayerState player1 = new PlayerState();

    @Builder.Default
    private PlayerState player2 = new PlayerState();

    //game phase
    private boolean gameStarted;
    private int leadingPlayer;
    private int currentTurn;
    private int round;

    @JsonIgnore
    @Deprecated
    public Long getPlayer1Id() { return player1.getUserId(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer1Score() { return player1.getScore(); }

    @JsonIgnore
    @Deprecated
    public boolean isPlayer1Ready() { return player1.isReady(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer1Go() { return player1.getGo(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer1GoScore() { return player1.getGoScore(); }

    @JsonIgnore
    @Deprecated
    public Long getPlayer2Id() { return player2.getUserId(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer2Score() { return player2.getScore(); }

    @JsonIgnore
    @Deprecated
    public boolean isPlayer2Ready() { return player2.isReady(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer2Go() { return player2.getGo(); }

    @JsonIgnore
    @Deprecated
    public int getPlayer2GoScore() { return player2.getGoScore(); }

    public GameState updatePlayerState(Player player, PlayerState newState) {
        if (player == Player.PLAYER_1) {
            return this.toBuilder().player1(newState).build();
        }
        return this.toBuilder().player2(newState).build();
    }

    public GameState updatePlayerState(Player player, UnaryOperator<PlayerState> updater) {
        if (player == Player.PLAYER_1) {
            return this.toBuilder()
                    .player1(updater.apply(this.player1))
                    .build();
        }
        return this.toBuilder()
                .player2(updater.apply(this.player2))
                .build();
    }

    @Builder.Default
    private GamePhase phase = GamePhase.IN_PROGRESS;
    private ChoiceInfo choiceInfo; // phase가 await류일때만 의미 있다.

    public Player getPlayerType(long userId) {
        if (Objects.equals(this.player1.getUserId(), userId)) return Player.PLAYER_1;
        if (Objects.equals(this.player2.getUserId(), userId)) return Player.PLAYER_2;
        throw new WebSocketBusinessException(WebSocketErrorCode.NOT_IN_ROOM);
    }

    public GameState withPlayerReady(Player player, boolean isReady) {
        if (player == Player.PLAYER_1) {
            return this.toBuilder()
                    .player1(this.player1.toBuilder().ready(isReady).build())
                    .build();
        } else {
            return this.toBuilder()
                    .player2(this.player2.toBuilder().ready(isReady).build())
                    .build();
        }
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
        return player == Player.PLAYER_1 ? player1.canGoStop() : player2.canGoStop();
    }

    @JsonIgnore
    public boolean isPlaying() {
        return this.phase == GamePhase.IN_PROGRESS;
    }
}