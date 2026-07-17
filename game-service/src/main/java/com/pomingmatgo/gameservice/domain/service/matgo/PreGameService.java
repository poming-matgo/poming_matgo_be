package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.*;
import com.pomingmatgo.gameservice.domain.card.Card;
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

    private static final int CARDS_TO_PICK = 5;
    private static final int PLAYER_CARD_COUNT = 10;
    private static final int REVEALED_CARD_COUNT = 8;
    private static final int PLAYER_1_END_INDEX = PLAYER_CARD_COUNT;
    private static final int PLAYER_2_END_INDEX = PLAYER_1_END_INDEX + PLAYER_CARD_COUNT;
    private static final int REVEALED_CARD_END_INDEX = PLAYER_2_END_INDEX + REVEALED_CARD_COUNT;

    //선 플레이어 정하는 과정
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

    public Mono<Boolean> isConfusedPlayer(long roomId, Player player) {
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

    /**
     * 선 선택 카드 저장. 선택 현황 조회(read)→검증→저장(write) 사이에 상대 선택이 끼어들면
     * 중복 월 검증이 뚫리므로 그 구간을 방 단위 락으로 직렬화한다 (joinRoom/leaveRoom과 같은 패턴).
     */
    public Mono<Void> selectLeaderCard(long roomId, Player player, int cardIndex) {
        return roomLockManager.withLock(roomId,
                leadingPlayerRepository.getCardByIndex(roomId, cardIndex)
                        .switchIfEmpty(Mono.error(new WebSocketBusinessException(INVALID_GAME_PHASE)))
                        .flatMap(card -> leadingPlayerRepository.getPlayerSelectedCard(roomId)
                                .doOnNext(choice -> choice.validateSelection(player, card.getMonth()))
                                .then(leadingPlayerRepository.savePlayerMonth(roomId, player, card.getMonth()))),
                () -> new WebSocketBusinessException(TOO_MANY_REQUESTS));
    }

    /**
     * 두 플레이어 모두 선택을 마쳤으면 후속 트리거를 claim한다 (true = 이 호출이 후속 진행 담당).
     * 락 불필요 — 월은 한 번 저장되면 불변이고, 동시 선택 완료로 둘 다 여기 도달해도
     * putIfAbsent 기반 트리거가 1회 발사를 보장한다.
     */
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
        return Flux.fromArray(Card.values())
                .collectList()
                .map(ArrayList::new)
                .doOnNext(Collections::shuffle)
                .map(this::dealCardsFromDeck)
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
        return Mono.zip(
                installedCardRepository.savePlayerCards(installedCard.getPlayer1(), roomId, Player.PLAYER_1),
                installedCardRepository.savePlayerCards(installedCard.getPlayer2(), roomId, Player.PLAYER_2),
                installedCardRepository.saveRevealedCard(installedCard.getRevealedCard(), roomId),
                installedCardRepository.saveHiddenCard(installedCard.getHiddenCard(), roomId)
        ).thenReturn(installedCard).retry(3);
    }

    public Mono<GameState> setFirstTurn(GameState gameState) {
        GameState.GameStateBuilder builder = gameState.toBuilder();
        GameState newState = builder.round(1).currentTurn(1).phase(IN_PROGRESS).build();

        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }
}