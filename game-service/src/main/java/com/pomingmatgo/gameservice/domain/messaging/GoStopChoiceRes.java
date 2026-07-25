package com.pomingmatgo.gameservice.domain.messaging;

import com.pomingmatgo.gameservice.domain.PlayerState;
import com.pomingmatgo.gameservice.domain.score.Payout;
import lombok.AllArgsConstructor;
import lombok.Getter;

// stopPayout은 지금 STOP할 경우의 정산 — 스톱 판단에 필요하므로 승자를 본인으로 가정해 박 계열까지 반영한다
@Getter
@AllArgsConstructor
public class GoStopChoiceRes {
    private final int nextGoNum;
    private final int score;
    private final Payout stopPayout;

    public static GoStopChoiceRes of(PlayerState playerState, Payout stopPayout) {
        return new GoStopChoiceRes(playerState.getGo() + 1, playerState.getScore(), stopPayout);
    }
}
