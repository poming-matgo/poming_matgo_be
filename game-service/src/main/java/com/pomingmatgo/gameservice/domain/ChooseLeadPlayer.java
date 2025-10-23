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
    public ChooseLeadPlayer selectMonthForPlayer(Player player, int selectedMonth) {
        ChooseLeadPlayer.ChooseLeadPlayerBuilder builder = this.toBuilder();

        switch (player) {
            case PLAYER_1:
                if (this.player1Month == NO_SELECTION) {
                    validateCardSelection(this.player2Month, selectedMonth);
                    builder.player1Month(selectedMonth);
                }
                break;
            case PLAYER_2:
                if (this.player2Month == NO_SELECTION) {
                    validateCardSelection(this.player1Month, selectedMonth);
                    builder.player2Month(selectedMonth);
                }
                break;
        }
        return builder.build();
    }

    private void validateCardSelection(int otherPlayerMonth, int selectedMonth) {
        if (otherPlayerMonth == selectedMonth) {
            throw new WebSocketBusinessException(ALREADY_SELECTED_CARD);
        }
    }
}
