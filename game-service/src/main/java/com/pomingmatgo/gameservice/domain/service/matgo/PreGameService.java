package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.ALREADY_SELECTED_CARD;

@Service
@RequiredArgsConstructor
public class PreGameService {
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final InstalledCardRepository installedCardRepository;
    private final GameStateRepository gameStateRepository;

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

    public Mono<Void> selectCard(RequestEvent<LeadSelectionReq> event, GameState gameState, Player player) {
        Long roomId = gameState.getRoomId();
        Mono<ChooseLeadPlayer> selectedCardsMono = leadingPlayerRepository.getPlayerSelectedCard(roomId);
        Mono<Card> curUserSelectedCardMono = leadingPlayerRepository.getCardByIndex(roomId, event.getData().getCardIndex());

        return Mono.zip(selectedCardsMono, curUserSelectedCardMono)
                .flatMap(tuple -> {
                    ChooseLeadPlayer chooseCards = tuple.getT1();
                    Card curUserSelectedCard = tuple.getT2();
                    ChooseLeadPlayer updatedChooseCards = chooseCards.selectMonthForPlayer(player, curUserSelectedCard.getMonth());
                    return leadingPlayerRepository.savePlayerSelectedCard(roomId, updatedChooseCards);
                });
    }


    public Mono<Boolean> isAllPlayerCardSelected(Long roomId) {
        return leadingPlayerRepository.getPlayerSelectedCard(roomId)
                .map(selectedCards -> selectedCards.getPlayer1Month() != 0 && selectedCards.getPlayer2Month() != 0);
    }

    public Mono<LeadSelectionRes> getLeadSelectionRes(Long roomId) {
        return leadingPlayerRepository.getPlayerSelectedCard(roomId)
                .zipWith(leadingPlayerRepository.getAllCards(roomId).collectList())
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
                installedCardRepository.savePlayer1Card(installedCard.getPlayer1(), roomId),
                installedCardRepository.savePlayer2Card(installedCard.getPlayer2(), roomId),
                installedCardRepository.saveRevealedCard(installedCard.getRevealedCard(), roomId),
                installedCardRepository.saveHiddenCard(installedCard.getHiddenCard(), roomId)
        ).thenReturn(installedCard).retry(3);
    }

    public Mono<GameState> setRoundInfo(GameState gameState) {
        GameState.GameStateBuilder builder = gameState.toBuilder();
        GameState newState = builder.round(1).currentTurn(1).build();

        return gameStateRepository.save(newState)
                .thenReturn(newState);
    }
}