package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.messaging.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.gamelog.GameCommandLog;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import com.pomingmatgo.gameservice.global.lock.RoomLockManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.pomingmatgo.gameservice.domain.GamePhase.IN_PROGRESS;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_GAME_PHASE;
import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.TOO_MANY_REQUESTS;

@Service
@RequiredArgsConstructor
@Slf4j
public class PreGameService {
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final InstalledCardRepository installedCardRepository;
    private final GameStateRepository gameStateRepository;
    private final RoomLockManager roomLockManager;
    private final GameCommandLog gameCommandLog;

    private static final int CARDS_TO_PICK = 5;
    private static final int PLAYER_CARD_COUNT = 10;
    private static final int REVEALED_CARD_COUNT = 8;
    private static final int PLAYER_1_END_INDEX = PLAYER_CARD_COUNT;
    private static final int PLAYER_2_END_INDEX = PLAYER_1_END_INDEX + PLAYER_CARD_COUNT;
    private static final int REVEALED_CARD_END_INDEX = PLAYER_2_END_INDEX + REVEALED_CARD_COUNT;

    //todo: cardService로 분리 및 cardsByMonth를 상수로
    public Mono<Void> pickFiveCardsAndSave(Long roomId) {
        Map<Integer, List<Card>> cardsByMonth = Arrays.stream(Card.values())
                .collect(Collectors.groupingBy(Card::getMonth));
        List<Integer> shuffledMonths = new ArrayList<>(cardsByMonth.keySet());
        Collections.shuffle(shuffledMonths);

        return Flux.fromIterable(shuffledMonths.subList(0, CARDS_TO_PICK))
                .map(month -> {
                    List<Card> cardsInMonth = cardsByMonth.get(month);
                    int randomIndex = ThreadLocalRandom.current().nextInt(cardsInMonth.size());
                    return cardsInMonth.get(randomIndex);
                })
                .collectList()
                .flatMap(selectedCards -> leadingPlayerRepository.saveSelectedCard(selectedCards, roomId));
    }

    /** 총통(같은 월 4장) 보유 여부 */
    public Mono<Boolean> hasChongtong(long roomId, Player player) {
        return installedCardRepository.getPlayerCards(roomId, player)
                .map(cardList -> {
                    if (cardList.isEmpty()) {
                        return false;
                    }
                    return cardList.stream()
                            .collect(Collectors.groupingBy(Card::getMonth, Collectors.counting()))
                            .values().stream()
                            .anyMatch(count -> count == 4);
                });
    }

    // read→검증→write 사이에 상대 선택이 끼어들면 중복 월 검증이 뚫리므로 방 단위 락으로 직렬화한다
    public Mono<Void> selectLeaderCard(long roomId, Player player, int cardIndex) {
        return roomLockManager.withLock(roomId,
                leadingPlayerRepository.getCardByIndex(roomId, cardIndex)
                        .switchIfEmpty(Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE)))
                        .flatMap(card -> leadingPlayerRepository.getPlayerSelectedCard(roomId)
                                .doOnNext(choice -> choice.validateSelection(player, card.getMonth()))
                                .then(leadingPlayerRepository.savePlayerMonth(roomId, player, card.getMonth()))),
                () -> new WebSocketBusinessException(TOO_MANY_REQUESTS));
    }

    /** true = 이 호출이 후속 진행 담당. 락 불필요 — putIfAbsent 트리거가 동시 도달에도 1회 발사를 보장한다 */
    public Mono<Boolean> checkAllSelected(long roomId) {
        return leadingPlayerRepository.getPlayerSelectedCard(roomId)
                .flatMap(choice -> choice.getPlayer1Month() != 0 && choice.getPlayer2Month() != 0
                        ? leadingPlayerRepository.tryClaimLeaderSelectionTrigger(roomId)
                        : Mono.just(false));
    }

    public Mono<LeadSelectionRes> getLeadSelectionRes(Long roomId) {
        return leadingPlayerRepository.getPlayerSelectedCard(roomId)
                .zipWith(leadingPlayerRepository.getAllCards(roomId))
                .map(tuple -> {
                    ChooseLeadPlayer chooseLeadPlayer = tuple.getT1();
                    List<Card> cards = tuple.getT2();

                    int player1Month = chooseLeadPlayer.getPlayer1Month();
                    int player2Month = chooseLeadPlayer.getPlayer2Month();
                    int leadPlayer = player1Month < player2Month ? 2 : 1;

                    LeadSelectionRes res = new LeadSelectionRes();
                    res.setPlayer1Month(player1Month);
                    res.setPlayer2Month(player2Month);
                    res.setLeadPlayer(leadPlayer);
                    res.setFiveCards(cards);

                    return res;
                });
    }

    public Mono<InstalledCard> distributeCards(long roomId) {
        return Mono.fromCallable(() -> {
                    List<Card> deck = new ArrayList<>(Arrays.asList(Card.values()));
                    Collections.shuffle(deck);
                    return deck;
                })
                .flatMap(deck -> distributeCards(roomId, deck)
                        // 배분 확정 후 셔플 덱을 로그 첫 레코드로 고정 — replay 경로(아래 seam)는 기록하지 않는다
                        .delayUntil(cards -> gameCommandLog.logDeckInit(roomId, deck)));
    }

    /** 셔플 결과를 값으로 받는 결정적 경로 — 커맨드 로그의 덱 고정 레코드와 replay가 이 seam을 쓴다 */
    public Mono<InstalledCard> distributeCards(long roomId, List<Card> deck) {
        return Mono.fromCallable(() -> dealCardsFromDeck(deck))
                .flatMap(installedCard -> persistAllCards(installedCard, roomId));
    }

    private InstalledCard dealCardsFromDeck(List<Card> shuffledDeck) {
        List<Card> player1 = new ArrayList<>(shuffledDeck.subList(0, PLAYER_1_END_INDEX));
        List<Card> player2 = new ArrayList<>(shuffledDeck.subList(PLAYER_1_END_INDEX, PLAYER_2_END_INDEX));
        List<Card> revealedCard = new ArrayList<>(shuffledDeck.subList(PLAYER_2_END_INDEX, REVEALED_CARD_END_INDEX));
        List<Card> hiddenCard = new ArrayList<>(shuffledDeck.subList(REVEALED_CARD_END_INDEX, shuffledDeck.size()));

        return new InstalledCard(player1, player2, revealedCard, hiddenCard);
    }

    private Mono<InstalledCard> persistAllCards(InstalledCard installedCard, long roomId) {
        // 저장이 append 방식이라 부분 성공 후 재시도 시 중복됨 → 매 시도 전 초기화로 멱등 보장
        return installedCardRepository.cleanup(roomId)
                .then(Mono.zip(
                        installedCardRepository.savePlayerCards(installedCard.getPlayer1(), roomId, Player.PLAYER_1),
                        installedCardRepository.savePlayerCards(installedCard.getPlayer2(), roomId, Player.PLAYER_2),
                        installedCardRepository.saveRevealedCard(installedCard.getRevealedCard(), roomId),
                        installedCardRepository.saveHiddenCard(installedCard.getHiddenCard(), roomId)
                ))
                .thenReturn(installedCard)
                .retry(3);
    }

    public Mono<GameState> setFirstTurn(GameState gameState) {
        GameState newState = gameState.toBuilder()
                .round(1).currentTurn(1).phase(IN_PROGRESS)
                .build();
        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }
}