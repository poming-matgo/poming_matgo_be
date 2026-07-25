package com.pomingmatgo.gameservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PlayerState implements Serializable {
    private static final int MIN_GO_STOP_SCORE = 7;
    private static final int GO_MULTIPLIER_FROM = 3;

    private Long userId;
    private boolean ready;
    private int score;
    private int go;
    private int goScore;

    @JsonIgnore
    public boolean canGoStop() {
        return score >= MIN_GO_STOP_SCORE && score > goScore;
    }

    /** 고 횟수만큼 가산 후 3고부터 고 1회당 2배 — 피박/광박 등 다른 배수도 여기에 곱한다 */
    public int payoutScore() {
        int bonused = score + go;
        return go < GO_MULTIPLIER_FROM ? bonused : bonused << (go - GO_MULTIPLIER_FROM + 1);
    }
}