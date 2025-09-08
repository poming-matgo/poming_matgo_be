package com.pomingmatgo.gameservice.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.redis.core.RedisHash;

import java.util.List;

@RedisHash(value = "gameState") //todo: 방 삭제되면 삭제되게 해야함
@Getter
@Setter
@NoArgsConstructor
public class GameState {
    long roomId;
    long player1Id;
    long player2Id;
    int round;
    int currentTurn;
    List<Integer> cardStack;
}
