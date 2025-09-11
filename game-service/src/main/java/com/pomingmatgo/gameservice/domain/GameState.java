package com.pomingmatgo.gameservice.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;
import java.util.List;

@RedisHash(value = "gameState") //todo: 방 삭제되면 삭제되게 해야함
@Getter
@Setter
@NoArgsConstructor
public class GameState implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    Long roomId;
    long player1Id;
    long player2Id;
    int round;
    int currentTurn;
    List<Integer> cardStack;
}
