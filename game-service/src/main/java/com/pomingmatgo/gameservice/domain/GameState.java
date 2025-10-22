package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode;
import lombok.*;
import org.springframework.data.annotation.Id;

import java.io.Serializable;

@Getter
@AllArgsConstructor
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
}
