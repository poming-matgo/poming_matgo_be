package com.example.gameservice.calculatescore;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.ScoreCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.calculatescore.ScoreCalculator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.BDDMockito.given;

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


}
