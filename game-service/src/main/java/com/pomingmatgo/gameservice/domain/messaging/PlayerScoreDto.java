package com.pomingmatgo.gameservice.domain.messaging;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PlayerScoreDto {
    private int playerNumber;
    private int score;
    private int go;
    private int payoutScore; // 고박은 승패가 갈려야 결정되므로 미반영
}