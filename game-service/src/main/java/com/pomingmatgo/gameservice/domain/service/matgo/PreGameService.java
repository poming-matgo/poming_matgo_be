package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.domain.card.Card;
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
}
