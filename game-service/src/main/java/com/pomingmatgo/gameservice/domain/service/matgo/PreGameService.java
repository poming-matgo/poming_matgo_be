package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.api.handler.event.RequestEvent;
import com.pomingmatgo.gameservice.api.request.WebSocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.SelectedCardRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PreGameService {
    private static final Random RANDOM = new Random();
    private final SelectedCardRepository selectedCardRepository;

    //선 플레이어 정하는 과정
    public Mono<Void> pickFiveCardsAndSave(Long roomId) {
        Map<Integer, List<Card>> cardsByMonth = Arrays.stream(Card.values())
                .collect(Collectors.groupingBy(Card::getMonth));

        List<Integer> months = new ArrayList<>(cardsByMonth.keySet());
        Collections.shuffle(months, RANDOM);
        List<Integer> selectedMonths = months.subList(0, 5);

        List<Card> selectedCards = selectedMonths.stream()
                .map(month -> {
                    List<Card> cards = cardsByMonth.get(month);
                    return cards.get(RANDOM.nextInt(cards.size()));
                })
                .toList();

        return selectedCardRepository.saveSelectedCard(selectedCards, roomId);
    }

    public Mono<Void> selectCard(RequestEvent<LeadSelectionReq> event) {

    }
}
