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
    private static final int MIN_WIN_SCORE = 7;
    private static final int PPEOK_WIN_COUNT = 3;

    private Long userId;
    private boolean ready;
    private int score;
    private int go;
    private int goScore;

    // 라운드가 아니라 한 게임 전체의 누적치다 (세번뻑 판정 기준)
    private int ppeokCount;

    // score와 항상 함께 갱신된다 (score == breakdown.total()) — 박 계열 배수 판정에 항목별 내역이 필요하다
    private ScoreBreakdown breakdown;

    @JsonIgnore
    public boolean canGoStop() {
        return score >= MIN_WIN_SCORE && score > goScore;
    }

    @JsonIgnore
    public boolean hasPpeokWin() {
        return ppeokCount >= PPEOK_WIN_COUNT;
    }

    /** 정산 기준 점수 — 세번뻑 승리는 카드 점수와 무관하게 7점 고정. getter 형태를 피해 Jackson 대상에서 제외 */
    public int winningScore() {
        return hasPpeokWin() ? MIN_WIN_SCORE : score;
    }
}