package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.*;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.ALREADY_SELECTED_CARD;

@Getter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ChooseLeadPlayer {

    private static final int NO_SELECTION = 0;

    int player1Month;
    int player2Month;

    /** 제시되는 5장이 서로 다른 월이라, 이 검증을 통과한 두 선택은 반드시 승부가 갈린다 */
    public void validateSelection(Player player, int selectedMonth) {
        int myMonth = (player == Player.PLAYER_1) ? this.player1Month : this.player2Month;
        int otherMonth = (player == Player.PLAYER_1) ? this.player2Month : this.player1Month;

        if (myMonth != NO_SELECTION || otherMonth == selectedMonth) {
            throw new WebSocketBusinessException(ALREADY_SELECTED_CARD);
        }
    }
}
