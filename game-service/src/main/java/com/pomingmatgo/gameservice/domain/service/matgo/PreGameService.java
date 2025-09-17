package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.response.websocket.LeadSelectionRes;
import com.pomingmatgo.gameservice.domain.InstalledCard;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.ALREADY_SELECTED_CARD;

@Service
@RequiredArgsConstructor
public class PreGameService {
    private static final Random RANDOM = new Random();
    private final LeadingPlayerRepository leadingPlayerRepository;
    private final InstalledCardRepository installedCardRepository;

    //선 플레이어 정하는 과정
    public Mono<Void> pickFiveCardsAndSave(Long roomId) {
        return Flux.fromArray(Card.values())
                .collectMultimap(Card::getMonth)
                .flatMapMany(cardsByMonth -> {
                    List<Integer> months = new ArrayList<>(cardsByMonth.keySet());
                    Collections.shuffle(months, RANDOM);
                    return Flux.fromIterable(months.subList(0, 5))
                            .map(month -> {
                                List<Card> cards = new ArrayList<>(cardsByMonth.get(month));
                                return cards.get(RANDOM.nextInt(cards.size()));
                            });
                })
                .collectList()
                .flatMap(selectedCards -> leadingPlayerRepository.saveSelectedCard(selectedCards, roomId));
    }

    public Mono<Void> selectCard(RequestEvent<LeadSelectionReq> event) {
        return leadingPlayerRepository.getPlayerSelectedCard(event.getRoomId())
                .flatMap(selectedCards -> leadingPlayerRepository.getCardByIndex(event.getRoomId(), event.getData().getCardIndex())
                        .flatMap(curUserSelectedCard -> {
                            int playerNum = event.getPlayerNum();
                            int selectedMonth = curUserSelectedCard.getMonth();

                            if (playerNum == 1 && selectedCards.getPlayer1Month() == 0) {
                                validateCardSelection(selectedCards.getPlayer2Month(), selectedMonth);
                                selectedCards.setPlayer1Month(selectedMonth);
                            } else if (playerNum == 2 && selectedCards.getPlayer2Month() == 0) {
                                validateCardSelection(selectedCards.getPlayer1Month(), selectedMonth);
                                selectedCards.setPlayer2Month(selectedMonth);
                            }

                            return leadingPlayerRepository.savePlayerSelectedCard(event.getRoomId(), selectedCards);
                        })
                );
    }

    private void validateCardSelection(int otherPlayerMonth, int selectedMonth) {
        if (otherPlayerMonth == selectedMonth) {
            throw new WebSocketBusinessException(ALREADY_SELECTED_CARD);
        }
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
                .flatMap(deck -> {
                    List<Card> player1 = new ArrayList<>(deck.subList(0, 10));
                    List<Card> player2 = new ArrayList<>(deck.subList(10, 20));
                    List<Card> revealedCard = new ArrayList<>(deck.subList(20, 28));
                    List<Card> hiddenCard = new ArrayList<>(deck.subList(28, deck.size()));

                    InstalledCard installedCard = new InstalledCard(player1, player2, revealedCard, hiddenCard);

                    return Mono.zip(
                            installedCardRepository.savePlayer1Card(player1, roomId),
                            installedCardRepository.savePlayer2Card(player2, roomId),
                            installedCardRepository.saveRevealedCard(revealedCard, roomId),
                            installedCardRepository.saveHiddenCard(hiddenCard, roomId)
                    ).thenReturn(installedCard);
                });
    }


}
