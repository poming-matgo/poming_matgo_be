package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.domain.leadingPlayer.ChooseLeadPlayer;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.LeadingPlayerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

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
                .flatMap(selectedCards ->
                        leadingPlayerRepository.getCardByIndex(event.getRoomId(), event.getData().getCardIndex())
                                .flatMap(curUserselectedCard -> {
                                    int playerNum = event.getPlayerNum();
                                    if (playerNum == 1 && selectedCards.getPlayer1Month() == 0) {
                                        if(selectedCards.getPlayer2Month() == curUserselectedCard.getMonth()){
                                            throw new IllegalArgumentException("이미 선택된 카드입니다.");
                                        }
                                        selectedCards.setPlayer1Month(curUserselectedCard.getMonth());
                                    } else if (playerNum == 2 && selectedCards.getPlayer2Month() == 0) {
                                        selectedCards.setPlayer2Month(curUserselectedCard.getMonth());
                                        if(selectedCards.getPlayer1Month() == curUserselectedCard.getMonth()){
                                            throw new IllegalArgumentException("이미 선택된 카드입니다.");
                                        }
                                    }

                                    return leadingPlayerRepository.savePlayerSelectedCard(event.getRoomId(), selectedCards);
                                })
                );
    }
}
