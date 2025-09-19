package com.example.gameservice.service;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;

import static com.pomingmatgo.gameservice.domain.card.Card.JAN_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class SubmitCardTest {
    @Mock
    private InstalledCardRepository installedCardRepository;

    @InjectMocks
    private GameService gameService;

    long roomId = 1;
    @Test
    void testSubmitCard1() {
        Mono<Card> submitCard = Mono.just(JAN_1);
        Mono<Card> topCard = Mono.just(Card.JAN_2);
        Flux<Card> revealedJanCard = Flux.just(Card.JAN_3);

        given(installedCardRepository.getTopCard(roomId))
                .willReturn(topCard);

        given(installedCardRepository.getRevealedCardByMonth(roomId, 1))
                .willReturn(revealedJanCard);

        given(installedCardRepository.saveRevealedCard(any(), eq(roomId)))
                .willReturn(Mono.just(true));

        StepVerifier.create(gameService.submitCard(roomId, submitCard))
                .verifyComplete();
    }

    @Test
    void testSubmitCard2() {
        Mono<Card> submitCard = Mono.just(JAN_1);
        Mono<Card> topCard = Mono.just(Card.JAN_2);
        Flux<Card> revealedJanCard = Flux.just(Card.JAN_3, Card.JAN_4);
        Flux<Card> retCard = Flux.just(JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4);

        given(installedCardRepository.getTopCard(roomId))
                .willReturn(topCard);

        given(installedCardRepository.getRevealedCardByMonth(roomId, 1))
                .willReturn(revealedJanCard);

        given(installedCardRepository.deleteAllRevealedCardByMonth(roomId, 1))
                .willReturn(Mono.just(true));

        StepVerifier.create(gameService.submitCard(roomId, submitCard))
                .recordWith(ArrayList::new) // 결과를 리스트로 기록
                .thenConsumeWhile(card -> true) // 모든 요소 소비
                .consumeRecordedWith(cards ->
                        assertThat(cards).containsExactlyInAnyOrderElementsOf(retCard.collectList().block())
                )
                .verifyComplete();
    }

    @Test
    void testSubmitCard3() {
        Mono<Card> submitCard = Mono.just(JAN_1);
        Mono<Card> topCard = Mono.just(Card.FEB_1);
        Flux<Card> revealedJanCard = Flux.empty();
        Flux<Card> revealedFebCard = Flux.just(Card.FEB_2);
        Flux<Card> retCard = Flux.just(Card.FEB_1, Card.FEB_2);

        given(installedCardRepository.getTopCard(roomId))
                .willReturn(topCard);

        given(installedCardRepository.getRevealedCardByMonth(roomId, 1))
                .willReturn(revealedJanCard);

        given(installedCardRepository.getRevealedCardByMonth(roomId, 2))
                .willReturn(revealedFebCard);

        given(installedCardRepository.deleteAllRevealedCardByMonth(anyLong(), anyInt()))
                .willReturn(Mono.just(true));

        given(installedCardRepository.saveRevealedCard(any(), eq(roomId)))
                .willReturn(Mono.just(true));

        StepVerifier.create(gameService.submitCard(roomId, submitCard))
                .recordWith(ArrayList::new)
                .thenConsumeWhile(card -> true)
                .consumeRecordedWith(cards ->
                        assertThat(cards).containsExactlyInAnyOrderElementsOf(retCard.collectList().block())
                )
                .verifyComplete();
    }
}
