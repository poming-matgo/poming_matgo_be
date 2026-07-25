package com.pomingmatgo.gameservice.domain.score;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

// 피박/광박/멍박은 "승자가 어느 항목으로 냈는가 + 패자가 몇 장인가"를 함께 봐야 하므로 총점만으로는 판정할 수 없다
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScoreBreakdown implements Serializable {
    private static final long serialVersionUID = 1L;

    private int piScore;
    private int gwangScore;
    private int kkutScore;
    private int ddiScore;

    // 피 장수는 쌍피를 2장으로 센 값 (피박 기준이 되는 수)
    private int piCount;
    private int gwangCount;
    private int kkutCount;
    private int ddiCount;

    // getter 형태를 피해 Jackson 직렬화 대상에서 제외한다 (PlayerState.score와 항상 같은 값)
    public int total() {
        return piScore + gwangScore + kkutScore + ddiScore;
    }

    public static ScoreBreakdown empty() {
        return ScoreBreakdown.builder().build();
    }
}
