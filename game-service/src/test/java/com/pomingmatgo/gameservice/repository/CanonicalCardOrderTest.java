package com.pomingmatgo.gameservice.repository;

import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InMemoryAcquiredCardRepository;
import com.pomingmatgo.gameservice.domain.repository.InMemoryInstalledCardRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// 읽기 순서가 삽입 이력과 무관하게 natural order로 고정돼야 replay가 성립한다 (선택지 인덱스·피 뺏기 tie-break)
@DisplayName("카드 저장소 canonical 정렬 계약")
class CanonicalCardOrderTest {

    private static final long ROOM_ID = 1L;

    @Test
    @DisplayName("바닥 월 스택은 삽입 순서와 무관하게 natural order로 반환된다")
    void revealedCardsReturnInNaturalOrderRegardlessOfInsertion() {
        InMemoryInstalledCardRepository repo = new InMemoryInstalledCardRepository();
        repo.saveRevealedCard(List.of(Card.JAN_4, Card.JAN_1), ROOM_ID).block();
        repo.saveRevealedCard(List.of(Card.JAN_3, Card.JAN_2), ROOM_ID).block();

        assertEquals(List.of(Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4),
                repo.getRevealedCardByMonth(ROOM_ID, 1).block());
    }

    @Test
    @DisplayName("바닥 전체 조회도 natural order로 반환된다")
    void allRevealedCardsReturnInNaturalOrder() {
        InMemoryInstalledCardRepository repo = new InMemoryInstalledCardRepository();
        repo.saveRevealedCard(List.of(Card.DEC_1, Card.FEB_3, Card.AUG_2, Card.JAN_1), ROOM_ID).block();

        assertEquals(List.of(Card.JAN_1, Card.FEB_3, Card.AUG_2, Card.DEC_1),
                repo.getAllRevealedCards(ROOM_ID).block());
    }

    @Test
    @DisplayName("획득 카드는 삽입 순서와 무관하게 natural order로 반환된다")
    void acquiredCardsReturnInNaturalOrderRegardlessOfInsertion() {
        InMemoryAcquiredCardRepository repo = new InMemoryAcquiredCardRepository();
        repo.addCards(ROOM_ID, 1, List.of(Card.NOV_1, Card.MAR_3)).block();
        repo.addCards(ROOM_ID, 1, List.of(Card.JAN_3, Card.JUL_4)).block();

        assertEquals(List.of(Card.JAN_3, Card.MAR_3, Card.JUL_4, Card.NOV_1),
                repo.getAllCards(ROOM_ID, 1).block());
    }
}
