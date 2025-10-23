package com.pomingmatgo.gameservice.api.response.websocket;

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
    private Player player;
}
