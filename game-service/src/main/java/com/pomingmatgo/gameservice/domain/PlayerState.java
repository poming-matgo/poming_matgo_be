package com.pomingmatgo.gameservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.pomingmatgo.gameservice.domain.score.ScoreBreakdown;
import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PlayerState implements Serializable {
    private static final int MIN_GO_STOP_SCORE = 7;

    private Long userId;
    private boolean ready;
    private int score;
    private int go;
    private int goScore;

    // score와 항상 함께 갱신된다 (score == breakdown.total()) — 박 계열 배수 판정에 항목별 내역이 필요하다
    private ScoreBreakdown breakdown;

    @JsonIgnore
    public boolean canGoStop() {
        return score >= MIN_GO_STOP_SCORE && score > goScore;
    }
}