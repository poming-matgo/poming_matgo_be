package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.Player;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AnnounceRoundRes {
    private int round;
    private int turn;
    private Player curPlayer;
    private long remainingMs;
}
