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
    private static final int MAX_ROUND = 10;
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

    @Builder.Default
    private GamePhase phase = GamePhase.IN_PROGRESS;
    private ChoiceInfo choiceInfo; // phase가 await류일때만 의미 있다.

    public PlayerState getPlayerState(Player player) {
        return player == Player.PLAYER_1 ? this.player1 : this.player2;
    }

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

    public Player getPlayerType(long userId) {
        if (Objects.equals(this.player1.getUserId(), userId)) return Player.PLAYER_1;
        if (Objects.equals(this.player2.getUserId(), userId)) return Player.PLAYER_2;
        throw new WebSocketBusinessException(WebSocketErrorCode.NOT_IN_ROOM);
    }

    public GameState withPlayerReady(Player player, boolean isReady) {
        return updatePlayerState(player, ps -> ps.toBuilder().ready(isReady).build());
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

    public boolean hasUser(long userId) {
        return Objects.equals(this.player1.getUserId(), userId) ||
                Objects.equals(this.player2.getUserId(), userId);
    }

    @JsonIgnore
    public boolean isRoomFull() {
        return this.player1.getUserId() != null && this.player2.getUserId() != null;
    }

    public boolean canJoin() {
        return this.player1.getUserId() == null || this.player2.getUserId() == null;
    }

    public GameState join(long userId) {
        if (this.player1.getUserId() == null) {
            return this.updatePlayerState(Player.PLAYER_1, ps -> ps.toBuilder().userId(userId).build());
        }
        if (this.player2.getUserId() == null) {
            return this.updatePlayerState(Player.PLAYER_2, ps -> ps.toBuilder().userId(userId).build());
        }

        throw new WebSocketBusinessException(WebSocketErrorCode.FULL_ROOM);
    }

    public boolean allPlayersReady() {
        return this.player1.isReady() && this.player2.isReady();
    }

    public GameState setNextTurn() {
        int nextTurn = (this.currentTurn == 1) ? 2 : 1;
        int nextRound = this.round + (this.currentTurn == 2 ? 1 : 0);

        return this.toBuilder()
                .currentTurn(nextTurn)
                .round(nextRound)
                .phase(GamePhase.IN_PROGRESS)
                .choiceInfo(null)
                .build();
    }

    @JsonIgnore
    public boolean isFinalRound() {
        return this.round == MAX_ROUND;
    }

    @JsonIgnore
    public boolean isLastTurn() {
        return isFinalRound() && this.currentTurn == 2;
    }
}