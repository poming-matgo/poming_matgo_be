package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.messaging.GameOverRes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("고 점수 처리 테스트")
class GoScoreTest {

    @Nested
    @DisplayName("고 보너스")
    class GoBonus {

        @ParameterizedTest(name = "카드 점수 {0}점 + {1}고 → {2}점")
        @CsvSource({
                "7, 0, 7",   // 고 없이 스톱하면 카드 점수 그대로
                "7, 1, 8",   // 1고 = +1
                "8, 2, 10",  // 2고 = +2
                "9, 3, 24",  // 3고 = (9+3)×2
                "9, 4, 52",  // 4고 = (9+4)×4
                "7, 5, 96"   // 5고 = (7+5)×8
        })
        void appliesBonusThenMultiplier(int score, int go, int expected) {
            assertThat(playerState(score, go).payoutScore()).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("승자 정산 점수(고박 포함)")
    class PayoutScore {

        @Test
        @DisplayName("상대가 고를 부른 적 없으면 승자의 고 보너스 점수가 그대로 정산 점수다")
        void withoutOpponentGo() {
            GameState state = stateOf(playerState(9, 1), playerState(3, 0));

            assertThat(state.isGoBak(Player.PLAYER_1)).isFalse();
            assertThat(state.payoutScoreOf(Player.PLAYER_1)).isEqualTo(10);
        }

        @Test
        @DisplayName("고를 부른 상대를 역전해 이기면 고박으로 승자 점수가 2배가 된다")
        void goBakDoublesWinnerScore() {
            GameState state = stateOf(playerState(7, 0), playerState(8, 2));

            assertThat(state.isGoBak(Player.PLAYER_1)).isTrue();
            assertThat(state.payoutScoreOf(Player.PLAYER_1)).isEqualTo(14);
        }

        @Test
        @DisplayName("양쪽 모두 고를 불렀어도 패자가 고를 불렀으므로 고박이 적용된다")
        void goBakAppliesWhenBothWentGo() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            // (9+3)×2 = 24, 고박 ×2
            assertThat(state.payoutScoreOf(Player.PLAYER_1)).isEqualTo(48);
        }

        @Test
        @DisplayName("무승부는 고 여부와 무관하게 0점이다")
        void drawScoresZero() {
            GameState state = stateOf(playerState(6, 1), playerState(5, 0));

            assertThat(state.isGoBak(Player.PLAYER_NOTHING)).isFalse();
            assertThat(state.payoutScoreOf(Player.PLAYER_NOTHING)).isZero();
        }
    }

    @Nested
    @DisplayName("GAME_OVER 응답")
    class GameOverPayload {

        @Test
        @DisplayName("승자·원점수·정산 점수·고 횟수·고박 여부를 함께 싣는다")
        void carriesBothRawAndMultipliedScore() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            GameOverRes res = GameOverRes.from(state, Player.PLAYER_1);

            assertThat(res.getWinner()).isEqualTo(Player.PLAYER_1);
            assertThat(res.getScore()).isEqualTo(9);
            assertThat(res.getPayoutScore()).isEqualTo(48);
            assertThat(res.getGoCount()).isEqualTo(3);
            assertThat(res.isGoBak()).isTrue();
        }

        @Test
        @DisplayName("무승부는 원점수·정산 점수 모두 0, 고박 false")
        void drawPayload() {
            GameState state = stateOf(playerState(6, 1), playerState(5, 2));

            GameOverRes res = GameOverRes.from(state, Player.PLAYER_NOTHING);

            assertThat(res.getWinner()).isEqualTo(Player.PLAYER_NOTHING);
            assertThat(res.getScore()).isZero();
            assertThat(res.getPayoutScore()).isZero();
            assertThat(res.getGoCount()).isZero();
            assertThat(res.isGoBak()).isFalse();
        }
    }

    /** goScore를 1 낮게 둬 고 이후 점수가 올라 스톱 가능해진 상태로 만든다 */
    private static PlayerState playerState(int score, int go) {
        return PlayerState.builder().score(score).go(go).goScore(go > 0 ? score - 1 : 0).build();
    }

    private static GameState stateOf(PlayerState player1, PlayerState player2) {
        return GameState.builder().roomId(1L).player1(player1).player2(player2).build();
    }
}
