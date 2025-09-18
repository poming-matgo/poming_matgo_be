package com.example.gameservice.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.service.matgo.calculatescore.ScoreCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ScoreCalculatorTest {

    @InjectMocks
    private ScoreCalculator scoreCalculator;

    long roomId = 1L;
    long playerNum = 1L;
    @Test
    void testCalculatePiScore1() {
        Flux<Card> testCards = Flux.just(
                Card.JAN_3, Card.JAN_4
        );

        StepVerifier.create(scoreCalculator.calculatePiScore(testCards))
                .expectNext(0)
                .verifyComplete();
    }

    @Test
    void testCalculatePiScore2() {
        Flux<Card> testCards = Flux.just(
                Card.JAN_3, Card.JAN_4, Card.FEB_3, Card.FEB_4, Card.MAR_3, Card.MAR_4, Card.APR_3, Card.APR_4,
                Card.NOV_2, Card.NOV_3
        ); // 피 8개 쌍피 2개 => 12개

        StepVerifier.create(scoreCalculator.calculatePiScore(testCards))
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void testCalculateGwangScore1() {
        //비삼광
        Flux<Card> testCards = Flux.just(
                Card.JAN_1, Card.MAR_1, Card.DEC_1
        );

        StepVerifier.create(scoreCalculator.calculateGwangScore(testCards))
                .expectNext(2)
                .verifyComplete();
    }

    @Test
    void testCalculateGwangScore2() {
        //삼광
        Flux<Card> testCards = Flux.just(
                Card.JAN_1, Card.MAR_1, Card.AUG_1
        );

        StepVerifier.create(scoreCalculator.calculateGwangScore(testCards))
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void testCalculateGwangScore3() {
        //사광 - 비광 포함
        Flux<Card> testCards = Flux.just(
                Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.DEC_1
        );

        StepVerifier.create(scoreCalculator.calculateGwangScore(testCards))
                .expectNext(4)
                .verifyComplete();
    }

    @Test
    void testCalculateGwangScore4() {
        //사광 + 비광 불포함
        Flux<Card> testCards = Flux.just(
                Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1
        );

        StepVerifier.create(scoreCalculator.calculateGwangScore(testCards))
                .expectNext(4)
                .verifyComplete();
    }

    @Test
    void testCalculateGwangScore5() {
        //오광
        Flux<Card> testCards = Flux.just(
                Card.JAN_1, Card.MAR_1, Card.AUG_1, Card.NOV_1, Card.DEC_1
        );

        StepVerifier.create(scoreCalculator.calculateGwangScore(testCards))
                .expectNext(15)
                .verifyComplete();
    }

    @Test
    void testCalculateKKUTScore1() {
        //고도리
        Flux<Card> testCards = Flux.just(
                Card.FEB_1, Card.APR_1, Card.AUG_2
        );

        StepVerifier.create(scoreCalculator.calculateKkutScore(testCards))
                .expectNext(5)
                .verifyComplete();
    }

    @Test
    void testCalculateKKUTScore2() {
        //고도리 + 일반 끗 3장
        Flux<Card> testCards = Flux.just(
                Card.FEB_1, Card.APR_1, Card.AUG_2, Card.MAY_1, Card.OCT_1, Card.DEC_2
        );

        StepVerifier.create(scoreCalculator.calculateKkutScore(testCards))
                .expectNext(7)
                .verifyComplete();
    }

    @Test
    void testCalculateKKUTScore3() {
        //일반 끗 6장
        Flux<Card> testCards = Flux.just(
                Card.APR_1, Card.AUG_2, Card.MAY_1, Card.JUN_1, Card.OCT_1, Card.DEC_2
        );

        StepVerifier.create(scoreCalculator.calculateKkutScore(testCards))
                .expectNext(2)
                .verifyComplete();
    }
}
