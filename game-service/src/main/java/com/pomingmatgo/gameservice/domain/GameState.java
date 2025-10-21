package com.pomingmatgo.gameservice.domain;

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
}
