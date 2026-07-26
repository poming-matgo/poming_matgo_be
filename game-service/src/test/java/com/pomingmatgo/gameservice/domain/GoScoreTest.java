package com.pomingmatgo.gameservice.domain;

import com.pomingmatgo.gameservice.domain.messaging.GameOverRes;
import com.pomingmatgo.gameservice.domain.messaging.GoStopChoiceRes;
import com.pomingmatgo.gameservice.domain.messaging.PlayerScoreDto;
import com.pomingmatgo.gameservice.domain.messaging.ScoreInfoRes;
import com.pomingmatgo.gameservice.domain.score.Multiplier;
import com.pomingmatgo.gameservice.domain.score.Payout;
import com.pomingmatgo.gameservice.domain.score.PayoutCalculator;
import com.pomingmatgo.gameservice.domain.score.ScoreBreakdown;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@DisplayName("고 점수 처리 테스트")
class GoScoreTest {

    private final PayoutCalculator payoutCalculator = new PayoutCalculator();

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
            GameState state = stateOf(playerState(score, go), playerState(0, 0));

            assertThat(payoutCalculator.provisionalPayout(state, Player.PLAYER_1).total()).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("승자 정산 점수(고박 포함)")
    class PayoutScore {

        @Test
        @DisplayName("상대가 고를 부른 적 없으면 승자의 고 보너스 점수가 그대로 정산 점수다")
        void withoutOpponentGo() {
            GameState state = stateOf(playerState(9, 1), playerState(3, 0));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.has(Multiplier.GO_BAK)).isFalse();
            assertThat(payout.total()).isEqualTo(10);
        }

        @Test
        @DisplayName("고를 부른 상대를 역전해 이기면 고박으로 승자 점수가 2배가 된다")
        void goBakDoublesWinnerScore() {
            GameState state = stateOf(playerState(7, 0), playerState(8, 2));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.has(Multiplier.GO_BAK)).isTrue();
            assertThat(payout.total()).isEqualTo(14);
        }

        @Test
        @DisplayName("양쪽 모두 고를 불렀어도 패자가 고를 불렀으므로 고박이 적용된다")
        void goBakAppliesWhenBothWentGo() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            // 기본 점수 9+3=12에 고 배수 ×2, 고박 ×2
            assertThat(payout.baseScore()).isEqualTo(12);
            assertThat(payout.multipliers())
                    .extracting(Payout.Applied::type, Payout.Applied::factor)
                    .containsExactly(tuple(Multiplier.GO_MULTIPLIER, 2), tuple(Multiplier.GO_BAK, 2));
            assertThat(payout.total()).isEqualTo(48);
        }

        @Test
        @DisplayName("무승부는 고 여부와 무관하게 0점이다")
        void drawScoresZero() {
            GameState state = stateOf(playerState(6, 1), playerState(5, 0));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_NOTHING)).isEqualTo(Payout.NONE);
        }

        @Test
        @DisplayName("진행 중 표시엔 승패가 갈려야 결정되는 고박(VERSUS)이 빠진다")
        void provisionalExcludesVersusMultipliers() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            Payout provisional = payoutCalculator.provisionalPayout(state, Player.PLAYER_1);

            assertThat(provisional.has(Multiplier.GO_BAK)).isFalse();
            assertThat(provisional.total()).isEqualTo(24);
        }
    }

    @Nested
    @DisplayName("피박/광박/멍박")
    class BakMultipliers {

        @Test
        @DisplayName("승자가 피로 점수를 냈고 패자 피가 7장 이하면 피박으로 2배가 된다")
        void piBakDoublesWinnerScore() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.piScore(2).piCount(11))),
                    playerState(3, 0, breakdown(b -> b.piCount(7))));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.has(Multiplier.PI_BAK)).isTrue();
            assertThat(payout.total()).isEqualTo(14);
        }

        @Test
        @DisplayName("패자 피가 8장이면 피박이 아니다 — 쌍피는 2장으로 센다")
        void piBakExcludedWhenLoserExceedsThreshold() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.piScore(2).piCount(11))),
                    playerState(3, 0, breakdown(b -> b.piCount(8))));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_1).has(Multiplier.PI_BAK)).isFalse();
        }

        @Test
        @DisplayName("승자가 피로 점수를 내지 못했으면 패자 피가 적어도 피박이 아니다")
        void piBakExcludedWhenWinnerScoredNoPi() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.piCount(9))),
                    playerState(3, 0, breakdown(b -> b.piCount(2))));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_1).has(Multiplier.PI_BAK)).isFalse();
        }

        @Test
        @DisplayName("승자가 광으로 점수를 냈고 패자가 광이 없으면 광박으로 2배가 된다")
        void gwangBakDoublesWinnerScore() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.gwangScore(3).gwangCount(3))),
                    playerState(3, 0, breakdown(b -> b.gwangCount(0))));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.has(Multiplier.GWANG_BAK)).isTrue();
            assertThat(payout.total()).isEqualTo(14);
        }

        @Test
        @DisplayName("패자가 광을 1장이라도 가졌으면 광박이 아니다")
        void gwangBakExcludedWhenLoserHasGwang() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.gwangScore(3).gwangCount(3))),
                    playerState(3, 0, breakdown(b -> b.gwangCount(1))));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_1).has(Multiplier.GWANG_BAK)).isFalse();
        }

        @Test
        @DisplayName("승자가 광으로 점수를 내지 못했으면 패자가 광이 없어도 광박이 아니다")
        void gwangBakExcludedWhenWinnerScoredNoGwang() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.gwangCount(2))),
                    playerState(3, 0, breakdown(b -> b.gwangCount(0))));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_1).has(Multiplier.GWANG_BAK)).isFalse();
        }

        @Test
        @DisplayName("끗을 7장 이상 모으면 상대 끗과 무관하게 멍박으로 2배가 된다")
        void mungBakDoublesWinnerScore() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.kkutScore(3).kkutCount(7))),
                    playerState(3, 0, breakdown(b -> b.kkutCount(4))));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.has(Multiplier.MUNG_BAK)).isTrue();
            assertThat(payout.total()).isEqualTo(14);
        }

        @Test
        @DisplayName("끗이 6장이면 멍박이 아니다")
        void mungBakExcludedBelowThreshold() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.kkutScore(2).kkutCount(6))),
                    playerState(3, 0, breakdown(b -> b.kkutCount(0))));

            assertThat(payoutCalculator.finalPayout(state, Player.PLAYER_1).has(Multiplier.MUNG_BAK)).isFalse();
        }

        @Test
        @DisplayName("고박·피박·광박·멍박은 함께 곱해진다")
        void bakMultipliersStack() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.piScore(2).piCount(11)
                            .gwangScore(3).gwangCount(3).kkutScore(3).kkutCount(7))),
                    playerState(3, 1, breakdown(b -> b.piCount(3))));

            Payout payout = payoutCalculator.finalPayout(state, Player.PLAYER_1);

            assertThat(payout.multipliers())
                    .extracting(Payout.Applied::type)
                    .containsExactly(Multiplier.GO_BAK, Multiplier.PI_BAK, Multiplier.GWANG_BAK, Multiplier.MUNG_BAK);
            assertThat(payout.total()).isEqualTo(112);
        }

        @Test
        @DisplayName("진행 중 표시엔 승패가 갈려야 결정되는 피박/광박이 빠지고, 자기 상태만 보는 멍박은 남는다")
        void provisionalKeepsOnlySelfScopedMungBak() {
            GameState state = stateOf(
                    playerState(7, 0, breakdown(b -> b.piScore(2).piCount(11)
                            .gwangScore(3).gwangCount(3).kkutScore(3).kkutCount(7))),
                    playerState(3, 0, breakdown(b -> b.piCount(3))));

            Payout provisional = payoutCalculator.provisionalPayout(state, Player.PLAYER_1);

            assertThat(provisional.multipliers())
                    .extracting(Payout.Applied::type)
                    .containsExactly(Multiplier.MUNG_BAK);
            assertThat(provisional.total()).isEqualTo(14);
        }
    }

    @Nested
    @DisplayName("GAME_OVER 응답")
    class GameOverPayload {

        @Test
        @DisplayName("승자·원점수·기본 점수·정산 점수·고 횟수·적용 배수를 함께 싣는다")
        void carriesBothRawAndMultipliedScore() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            GameOverRes res = GameOverRes.from(state, Player.PLAYER_1,
                    payoutCalculator.finalPayout(state, Player.PLAYER_1));

            assertThat(res.getWinner()).isEqualTo(Player.PLAYER_1);
            assertThat(res.getScore()).isEqualTo(9);
            assertThat(res.getBaseScore()).isEqualTo(12);
            assertThat(res.getPayoutScore()).isEqualTo(48);
            assertThat(res.getGoCount()).isEqualTo(3);
            assertThat(res.getMultipliers())
                    .extracting(Payout.Applied::type)
                    .containsExactly(Multiplier.GO_MULTIPLIER, Multiplier.GO_BAK);
            assertThat(res.isGoBak()).isTrue();
        }

        @Test
        @DisplayName("무승부는 원점수·정산 점수 모두 0, 배수 없음")
        void drawPayload() {
            GameState state = stateOf(playerState(6, 1), playerState(5, 2));

            GameOverRes res = GameOverRes.from(state, Player.PLAYER_NOTHING,
                    payoutCalculator.finalPayout(state, Player.PLAYER_NOTHING));

            assertThat(res.getWinner()).isEqualTo(Player.PLAYER_NOTHING);
            assertThat(res.getScore()).isZero();
            assertThat(res.getPayoutScore()).isZero();
            assertThat(res.getGoCount()).isZero();
            assertThat(res.getMultipliers()).isEmpty();
            assertThat(res.isGoBak()).isFalse();
        }
    }

    @Nested
    @DisplayName("GO_STOP_CHOICE 응답")
    class GoStopChoicePayload {

        @Test
        @DisplayName("지금 스톱할 경우의 정산을 싣는다 — 스톱 판단에 필요하므로 고박(VERSUS)까지 반영한다")
        void carriesStopPayoutIncludingVersusMultipliers() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            GoStopChoiceRes res = GoStopChoiceRes.of(state.getPlayerState(Player.PLAYER_1),
                    payoutCalculator.finalPayout(state, Player.PLAYER_1));

            assertThat(res.getNextGoNum()).isEqualTo(4);
            assertThat(res.getScore()).isEqualTo(9);
            // 같은 상태의 SCORE_UPDATE(provisional)는 24 — 고박이 빠져 스톱 판단 근거가 되지 못한다
            assertThat(res.getStopPayout().has(Multiplier.GO_BAK)).isTrue();
            assertThat(res.getStopPayout().total()).isEqualTo(48);
        }
    }

    @Nested
    @DisplayName("SCORE_UPDATE 응답")
    class ScoreUpdatePayload {

        @Test
        @DisplayName("플레이어별 원점수와 정산 점수를 둘 다 싣는다")
        void carriesBothRawAndPayoutScore() {
            GameState state = stateOf(playerState(9, 3), playerState(8, 1));

            List<PlayerScoreDto> scores = ScoreInfoRes.from(state,
                    payoutCalculator.provisionalPayout(state, Player.PLAYER_1),
                    payoutCalculator.provisionalPayout(state, Player.PLAYER_2)).getScores();

            assertThat(scores).extracting(
                            PlayerScoreDto::getPlayerNumber,
                            PlayerScoreDto::getScore,
                            PlayerScoreDto::getGo,
                            PlayerScoreDto::getPayoutScore)
                    .containsExactly(
                            tuple(1, 9, 3, 24),  // (9+3)×2
                            tuple(2, 8, 1, 9));  // 8+1
        }
    }

    /** goScore를 1 낮게 둬 고 이후 점수가 올라 스톱 가능해진 상태로 만든다 */
    private static PlayerState playerState(int score, int go) {
        return PlayerState.builder().score(score).go(go).goScore(go > 0 ? score - 1 : 0).build();
    }

    private static PlayerState playerState(int score, int go, ScoreBreakdown breakdown) {
        return playerState(score, go).toBuilder().breakdown(breakdown).build();
    }

    private static ScoreBreakdown breakdown(UnaryOperator<ScoreBreakdown.ScoreBreakdownBuilder> customizer) {
        return customizer.apply(ScoreBreakdown.builder()).build();
    }

    private static GameState stateOf(PlayerState player1, PlayerState player2) {
        return GameState.builder().roomId(1L).player1(player1).player2(player2).build();
    }
}
