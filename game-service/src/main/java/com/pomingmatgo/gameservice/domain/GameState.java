package com.pomingmatgo.gameservice.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
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
}
