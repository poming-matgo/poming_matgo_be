package com.pomingmatgo.gameservice.api.response.websocket;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AnnounceRoundRes {
    private int round;
    private int turn;
    private int player;
}
