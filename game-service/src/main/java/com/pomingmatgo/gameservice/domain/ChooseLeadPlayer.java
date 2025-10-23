package com.pomingmatgo.gameservice.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder(toBuilder = true)
public class ChooseLeadPlayer {
    int player1Month;
    int player2Month;
}