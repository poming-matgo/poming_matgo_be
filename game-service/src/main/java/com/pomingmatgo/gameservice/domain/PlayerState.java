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

    private Long userId;
    private boolean ready;
    private int score;
    private int go;
    private int goScore;

    @JsonIgnore
    public boolean canGoStop() {
        return score >= MIN_GO_STOP_SCORE && score > goScore;
    }
}