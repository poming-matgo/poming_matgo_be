package com.pomingmatgo.gameservice.service;

import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.repository.AcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.CardMatchEngine;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static com.pomingmatgo.gameservice.domain.Player.PLAYER_1;
import static com.pomingmatgo.gameservice.domain.card.Card.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

// 이월이 없으면 바닥에서 이미 삭제된 획득 카드가 어느 플레이어에게도 귀속되지 않고 유실된다
@ExtendWith(MockitoExtension.class)
@DisplayName("바닥 카드 선택 대기 시 기획득 카드 이월/복원")
class FloorChoiceCarryoverTest {

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
    @DisplayName("낸 카드로 획득한 뒤 뒤집은 카드가 선택 대기를 만들면 획득분이 choiceInfo.prevCards로 이월된다")
    void carryAcquiredCardsIntoChoiceInfo() {
        // 낸 카드(JAN_1): 1월 바닥 1장 → 즉시 획득 / 뒤집은 카드(FEB_1): 2월 바닥 2장 → 선택 대기
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 1))
                .willReturn(Mono.just(List.of(JAN_2)));
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 2))
                .willReturn(Mono.just(List.of(FEB_2, FEB_3)));
        given(installedCardRepository.deleteAllRevealedCardByMonth(ROOM_ID, 1))
                .willReturn(Mono.just(true));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of()));
        given(gameStateRepository.save(any())).willReturn(Mono.just(ROOM_ID));

        StepVerifier.create(gameService.submitCard(inProgressState(), JAN_1, FEB_1))
                .assertNext(result -> assertThat(result.isChoiceRequired()).isTrue())
                .verifyComplete();

        ArgumentCaptor<GameState> captor = ArgumentCaptor.forClass(GameState.class);
        then(gameStateRepository).should().save(captor.capture());
        ChoiceInfo saved = captor.getValue().getChoiceInfo();

        assertThat(saved.getPrevCards()).containsExactlyInAnyOrder(JAN_1, JAN_2);
        assertThat(saved.getPrevMoveCards()).isEmpty();
    }

    @Test
    @DisplayName("선택 완료 시 이월된 획득/피 뺏기가 최종 결과에 복원된다")
    void restoreCarriedCardsOnSelection() {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(PLAYER_1)
                .submittedCard(FEB_1)
                .selectableCards(List.of(FEB_2, FEB_3))
                .turnedCard(null)
                .prevCards(List.of(JAN_2, JAN_1, NOV_4))
                .prevMoveCards(List.of(NOV_4))
                .build();
        GameState state = inProgressState().toBuilder()
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(choiceInfo)
                .build();

        given(installedCardRepository.deleteRevealedCard(anyLong(), any()))
                .willReturn(Mono.just(true));
        given(installedCardRepository.getAllRevealedCards(ROOM_ID)).willReturn(Mono.just(List.of(FEB_3)));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of()));
        given(gameStateRepository.save(any())).willReturn(Mono.just(ROOM_ID));

        StepVerifier.create(gameService.selectFloorCard(state, PLAYER_1, 0))
                .assertNext(result -> {
                    assertThat(result.isChoiceRequired()).isFalse();
                    assertThat(result.getAcquiredCards())
                            .containsExactlyInAnyOrder(JAN_1, JAN_2, NOV_4, FEB_1, FEB_2);
                    // 뺏은 피가 moveCards로 복원되지 않으면 loseCard가 생략돼 양쪽 플레이어에 중복 귀속된다
                    assertThat(result.getMoveCards()).containsExactly(NOV_4);
                })
                .verifyComplete();

        ArgumentCaptor<GameState> captor = ArgumentCaptor.forClass(GameState.class);
        then(gameStateRepository).should().save(captor.capture());
        assertThat(captor.getValue().getPhase()).isEqualTo(GamePhase.IN_PROGRESS);
        assertThat(captor.getValue().getChoiceInfo()).isNull();
    }

    @Test
    @DisplayName("연속 선택(뒤집은 카드가 또 선택 대기)이면 새 choiceInfo가 기존 이월분+이번 선택 획득분을 물려받는다")
    void accumulateCarryoverOnConsecutiveChoice() {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(PLAYER_1)
                .submittedCard(FEB_1)
                .selectableCards(List.of(FEB_2, FEB_3))
                .turnedCard(MAR_1)
                .prevCards(List.of(JAN_2, JAN_1))
                .build();
        GameState state = inProgressState().toBuilder()
                .phase(GamePhase.AWAITING_FLOOR_CARD_CHOICE)
                .choiceInfo(choiceInfo)
                .build();

        // 뒤집어둔 카드(MAR_1): 3월 바닥 2장 → 두 번째 선택 대기
        given(installedCardRepository.getRevealedCardByMonth(ROOM_ID, 3))
                .willReturn(Mono.just(List.of(MAR_2, MAR_3)));
        given(installedCardRepository.deleteRevealedCard(anyLong(), any()))
                .willReturn(Mono.just(true));
        given(acquiredCardRepository.getAllCards(ROOM_ID, 2)).willReturn(Mono.just(List.of()));
        given(gameStateRepository.save(any())).willReturn(Mono.just(ROOM_ID));

        StepVerifier.create(gameService.selectFloorCard(state, PLAYER_1, 0))
                .assertNext(result -> assertThat(result.isChoiceRequired()).isTrue())
                .verifyComplete();

        ArgumentCaptor<GameState> captor = ArgumentCaptor.forClass(GameState.class);
        then(gameStateRepository).should().save(captor.capture());
        ChoiceInfo saved = captor.getValue().getChoiceInfo();

        assertThat(saved.getSubmittedCard()).isEqualTo(MAR_1);
        assertThat(saved.getSelectableCards()).containsExactlyInAnyOrder(MAR_2, MAR_3);
        assertThat(saved.getPrevCards()).containsExactlyInAnyOrder(JAN_1, JAN_2, FEB_1, FEB_2);
    }
}
