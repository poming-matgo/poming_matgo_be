package com.pomingmatgo.gameservice.infrastructure.repository.inmemory;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryLeadingPlayerRepositoryTest {
    private final InMemoryLeadingPlayerRepository repository = new InMemoryLeadingPlayerRepository();

    @Test
    void cleanupResetsSelectionWithoutAffectingOtherRooms() {
        for (long roomId : List.of(1L, 2L)) {
            repository.saveSelectedCard(List.of(Card.JAN_1, Card.FEB_1), roomId).block();
            repository.savePlayerMonth(roomId, Player.PLAYER_1, 1).block();
            repository.savePlayerMonth(roomId, Player.PLAYER_2, 2).block();
        }
        repository.cleanup(1L).block();

        assertTrue(repository.getAllCards(1L).block().isEmpty());
        var cleared = repository.getPlayerSelectedCard(1L).block();
        assertEquals(0, cleared.getPlayer1Month());
        assertEquals(0, cleared.getPlayer2Month());
        assertEquals(List.of(Card.JAN_1, Card.FEB_1), repository.getAllCards(2L).block());
        var preserved = repository.getPlayerSelectedCard(2L).block();
        assertEquals(1, preserved.getPlayer1Month());
        assertEquals(2, preserved.getPlayer2Month());

        repository.saveSelectedCard(List.of(Card.MAR_1), 1L).block();
        repository.savePlayerMonth(1L, Player.PLAYER_1, 3).block();
        assertEquals(List.of(Card.MAR_1), repository.getAllCards(1L).block());
        assertEquals(3, repository.getPlayerSelectedCard(1L).block().getPlayer1Month());
        assertEquals(0, repository.getPlayerSelectedCard(1L).block().getPlayer2Month());
    }
}
