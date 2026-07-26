package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import com.pomingmatgo.gameservice.domain.service.matgo.SpecialEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.card.Card.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@DisplayName("판쓸이 — 바닥을 전부 가져가면 상대 피 1장")
class SweepTest {

    @Mock
    private InstalledCardRepository installedCardRepository;
    @Mock
    private GameStateRepository gameStateRepository;
    @Mock
    private AcquiredCardRepository acquiredCardRepository;
    @Spy
    private CardMatchEngine cardMatchEngine = new CardMatchEngine();

    @InjectMocks
    private GameService gameService;

    private static final long ROOM_ID = 1L;

    private GameState inProgressState() {
        return GameState.builder()
                .roomId(ROOM_ID)
                .leadingPlayer(1)
                .currentTurn(1)
                .round(1)
                .phase(GamePhase.IN_PROGRESS)
                .build();
    }

    @Test
    @DisplayName("낸 카드와 뒤집은 카드가 바닥을 모두 쓸면 판쓸이로 상대 피를 가져온다")
    void sweepClearsFloor() {
        // 바닥은 1월/2월 각 1장뿐 — 두 장 다 매칭돼 바닥이 0장이 된다
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 1)).willReturn(Mono.just(List.of(JAN_2)));
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 2)).willReturn(Mono.just(List.of(FEB_2)));
        given(installedCardRepository.deleteAllRevealedCardByMonth(anyLong(), anyInt())).willReturn(Mono.just(true));
        given(installedCardRepository.getAllRevealedCards(ROOM_ID)).willReturn(Mono.just(List.of()));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of(MAR_3)));

        StepVerifier.create(gameService.submitCard(inProgressState(), JAN_1, FEB_1))
                .assertNext(result -> {
                    assertThat(result.getSpecialEvents()).containsExactly(SpecialEvent.SWEEP);
                    assertThat(result.getAcquiredCards())
                            .containsExactlyInAnyOrder(JAN_1, JAN_2, FEB_1, FEB_2, MAR_3);
                    // moveCards가 있어야 settleTurn이 상대에게서 그 피를 회수한다
                    assertThat(result.getMoveCards()).containsExactly(MAR_3);
                    assertThat(result.isClaimOpponentPi()).isTrue();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("바닥에 카드가 남으면 판쓸이가 아니다")
    void noSweepWhenFloorRemains() {
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 1)).willReturn(Mono.just(List.of(JAN_2)));
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 2)).willReturn(Mono.just(List.of()));
        given(installedCardRepository.deleteAllRevealedCardByMonth(anyLong(), anyInt())).willReturn(Mono.just(true));
        given(installedCardRepository.saveRevealedCard(List.of(FEB_1), ROOM_ID)).willReturn(Mono.just(true));
        given(installedCardRepository.getAllRevealedCards(ROOM_ID)).willReturn(Mono.just(List.of(FEB_1)));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of(MAR_3)));

        StepVerifier.create(gameService.submitCard(inProgressState(), JAN_1, FEB_1))
                .assertNext(result -> {
                    assertThat(result.getSpecialEvents()).isEmpty();
                    assertThat(result.getMoveCards()).isEmpty();
                    assertThat(result.getAcquiredCards()).containsExactlyInAnyOrder(JAN_1, JAN_2);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("쪽과 겹치면 서로 다른 피를 두 장 가져온다")
    void sweepStacksWithJjok() {
        // 빈 바닥에서 낸 카드와 뒤집은 카드가 같은 월 → 쪽. 둘 다 획득해 바닥은 계속 0장이다
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 1)).willReturn(Mono.just(List.of()));
        given(installedCardRepository.deleteAllRevealedCardByMonth(anyLong(), anyInt())).willReturn(Mono.just(true));
        given(installedCardRepository.getAllRevealedCards(ROOM_ID)).willReturn(Mono.just(List.of()));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of(MAR_3, MAR_4)));

        StepVerifier.create(gameService.submitCard(inProgressState(), JAN_1, JAN_2))
                .assertNext(result -> {
                    assertThat(result.getSpecialEvents())
                            .containsExactly(SpecialEvent.JJOK, SpecialEvent.SWEEP);
                    assertThat(result.getMoveCards()).containsExactlyInAnyOrder(MAR_3, MAR_4);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("상대에게 피가 없으면 판쓸이 보상도 이벤트도 없다")
    void noSweepRewardWithoutOpponentPi() {
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 1)).willReturn(Mono.just(List.of(JAN_2)));
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 2)).willReturn(Mono.just(List.of(FEB_2)));
        given(installedCardRepository.deleteAllRevealedCardByMonth(anyLong(), anyInt())).willReturn(Mono.just(true));
        given(installedCardRepository.getAllRevealedCards(ROOM_ID)).willReturn(Mono.just(List.of()));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of(MAY_1)));

        StepVerifier.create(gameService.submitCard(inProgressState(), JAN_1, FEB_1))
                .assertNext(result -> {
                    assertThat(result.getSpecialEvents()).isEmpty();
                    assertThat(result.getMoveCards()).isEmpty();
                })
                .verifyComplete();
    }
}
