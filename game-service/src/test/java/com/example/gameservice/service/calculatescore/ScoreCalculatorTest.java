package com.example.gameservice.service.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.calculatescore.ScoreCalculator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.stream.Stream;

@DisplayName("ScoreCalculator 점수 계산 로직 테스트")
class ScoreCalculatorTest {
    @Autowired
    private ScoreCalculator scoreCalculator;

    // === 피(Pi) 점수 계산 테스트 ======================================================
    static Stream<Arguments> piScoreTestCases() {
        return Stream.of(
                Arguments.of("피가 10장 미만일 경우", List.of(Card.JAN_3, Card.JAN_4), 0),
                Arguments.of("쌍피 2개를 포함하여 총 12장일 경우", List.of(
                        Card.JAN_3, Card.JAN_4, Card.FEB_3, Card.FEB_4, Card.MAR_3, Card.MAR_4, Card.APR_3, Card.APR_4,
                        Card.NOV_4, Card.DEC_4 // 쌍피 2개 (NOV, DEC)
                ), 3) // 12(장) - 10 + 1 = 3점
        );
    }

    @DisplayName("피 점수를 정확하게 계산해야 한다")
    @ParameterizedTest(name = "[{index}] {0}: {2}점")
    @MethodSource("piScoreTestCases")
    void calculatePiScore_shouldReturnCorrectScore(String caseName, List<Card> cards, int expectedScore) {
        StepVerifier.create(scoreCalculator.calculatePiScore(cards))
                .expectNext(expectedScore)
                .verifyComplete();
    }


    // === 광(Gwang) 점수 계산 테스트 =====================================================
    static Stream<Arguments> gwangScoreTestCases() {
        return Stream.of(
                Arguments.of("비광 포함 3광 (비삼광)", List.of(Card.JAN_1, Card.MAR_1, Card.DEC_1), 2),
                Arguments.of("비광 미포함 3광 (삼광)", List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1), 3),
                Arguments.of("비광 포함 4광", List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.DEC_1), 4),
                Arguments.of("비광 미포함 4광", List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1), 4),
                Arguments.of("5광 (오광)", List.of(Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1), 15)
        );
    }

    @DisplayName("광 점수를 정확하게 계산해야 한다")
    @ParameterizedTest(name = "[{index}] {0}: {2}점")
    @MethodSource("gwangScoreTestCases")
    void calculateGwangScore_shouldReturnCorrectScore(String caseName, List<Card> cards, int expectedScore) {
        StepVerifier.create(scoreCalculator.calculateGwangScore(cards))
                .expectNext(expectedScore)
                .verifyComplete();
    }


    // === 끗(Kkut) 점수 계산 테스트 ======================================================
    static Stream<Arguments> kkutScoreTestCases() {
        return Stream.of(
                Arguments.of("고도리 (새 3마리)", List.of(Card.FEB_1, Card.APR_1, Card.AUG_2), 5),
                Arguments.of("고도리 + 일반 끗 3장 (총 6장)", List.of(
                        Card.FEB_1, Card.APR_1, Card.AUG_2, // 고도리
                        Card.MAY_1, Card.OCT_1, Card.DEC_2
                ), 7), // 고도리 5점 + (6장-5장)*1점 + 1점 = 7점
                Arguments.of("고도리 없이 일반 끗 6장", List.of(
                        Card.APR_1, Card.AUG_2, Card.MAY_1, Card.JUN_1, Card.OCT_1, Card.DEC_2
                ), 2) // (6장-5장)*1점 + 1점 = 2점
        );
    }

    @DisplayName("끗(열끗) 점수를 정확하게 계산해야 한다")
    @ParameterizedTest(name = "[{index}] {0}: {2}점")
    @MethodSource("kkutScoreTestCases")
    void calculateKkutScore_shouldReturnCorrectScore(String caseName, List<Card> cards, int expectedScore) {
        StepVerifier.create(scoreCalculator.calculateKkutScore(cards))
                .expectNext(expectedScore)
                .verifyComplete();
    }


    // === 띠(Ddi) 점수 계산 테스트 =======================================================
    static Stream<Arguments> ddiScoreTestCases() {
        return Stream.of(
                Arguments.of("띠 6장 (단일 족보 없음)", List.of(
                        Card.JAN_2, Card.FEB_2, Card.APR_2, Card.MAY_2, Card.JUN_2, Card.SEP_1
                ), 2), // (6장-5장)*1점 + 1점 = 2점
                Arguments.of("홍단, 초단 완성 + 띠 2장 (총 8장)", List.of(
                        Card.JAN_2, Card.FEB_2, Card.MAR_2, // 홍단
                        Card.APR_2, Card.MAY_2,             // 청단(미완성)
                        Card.JUN_2, Card.JUL_2, Card.SEP_1  // 초단
                ), 10), // 홍단 3점 + 초단 3점 + (8장-5장)*1점 + 1점 = 3+3+4=10점
                Arguments.of("홍단, 청단, 초단 모두 완성 + 일반 띠 1장 (총 10장)", List.of(
                        Card.JAN_2, Card.FEB_2, Card.MAR_2, // 홍단
                        Card.APR_2, Card.MAY_2, Card.JUN_2, // 청단
                        Card.JUL_2, Card.SEP_1, Card.OCT_2, // 초단
                        Card.DEC_3
                ), 15) // 홍단 3점 + 청단 3점 + 초단 3점 + (10장-5장)*1점 + 1점 = 3+3+3+6=15점
        );
    }

    @DisplayName("띠 점수를 정확하게 계산해야 한다")
    @ParameterizedTest(name = "[{index}] {0}: {2}점")
    @MethodSource("ddiScoreTestCases")
    void calculateDdiScore_shouldReturnCorrectScore(String caseName, List<Card> cards, int expectedScore) {
        StepVerifier.create(scoreCalculator.calculateDdiScore(cards))
                .expectNext(expectedScore)
                .verifyComplete();
    }
}
