package com.example.gameservice;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class ConfusedPlayerTest {

    @Mock
    private InstalledCardRepository installedCardRepository;

    @InjectMocks
    private GameService gameService;
    long roomId = 1L;


    Player player1 = Player.PLAYER_1;
    Player player2 = Player.PLAYER_2;

    @Test
    void testIsConfusedPlayer1() {
        Flux<Card> player1Cards = Flux.just(
                Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4, Card.FEB_1, Card.FEB_2, Card.FEB_3, Card.MAR_1, Card.APR_2, Card.AUG_1
        );

        given(installedCardRepository.getPlayerCards(roomId, player1))
                .willReturn(player1Cards);

        StepVerifier.create(gameService.isConfusedPlayer(roomId, player1))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void testIsConfusedPlayer2() {
        Flux<Card> player1Cards = Flux.just(
                Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4, Card.FEB_1, Card.FEB_2, Card.FEB_3, Card.FEB_4, Card.APR_2, Card.AUG_1
        );

        given(installedCardRepository.getPlayerCards(roomId, player2))
                .willReturn(player1Cards);

        StepVerifier.create(gameService.isConfusedPlayer(roomId, player2))
                .expectNext(true)
                .verifyComplete();
    }
    @Test
    void testIsNotConfusedPlayer() {
        Flux<Card> player1Cards = Flux.just(
                Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.FEB_1, Card.FEB_2, Card.FEB_3, Card.MAR_1, Card.APR_2, Card.AUG_1, Card.AUG_2
        );

        given(installedCardRepository.getPlayerCards(roomId, player1))
                .willReturn(player1Cards);

        StepVerifier.create(gameService.isConfusedPlayer(roomId, player1))
                .expectNext(false)
                .verifyComplete();
    }
}
