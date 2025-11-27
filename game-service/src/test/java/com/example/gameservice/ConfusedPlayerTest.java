package com.example.gameservice;

import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import com.pomingmatgo.gameservice.domain.service.matgo.GameService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.stream.Stream;

import static org.mockito.BDDMockito.given;

@DisplayName("GameService 총통(Chongtong) 판정 로직 테스트")
@ExtendWith(MockitoExtension.class)
class ConfusedPlayerTest {

    @Mock
    private InstalledCardRepository installedCardRepository;

    @InjectMocks
    private GameService gameService;

    private static final long ROOM_ID = 1L;
    private static final Player PLAYER_1 = Player.PLAYER_1;
    private static final Player PLAYER_2 = Player.PLAYER_2;

    static Stream<Arguments> chongtongTestCases() {
        List<Card> chongtongHand = List.of(
                Card.JAN_1, Card.JAN_2, Card.JAN_3, Card.JAN_4, // 1월 4장 (총통)
                Card.FEB_1, Card.FEB_2, Card.FEB_3, Card.MAR_1, Card.APR_2, Card.AUG_1
        );

        List<Card> normalHand = List.of(
                Card.JAN_1, Card.JAN_2, Card.JAN_3, // 1월 3장
                Card.FEB_1, Card.FEB_2, Card.FEB_3, Card.MAR_1, Card.APR_2, Card.AUG_1, Card.AUG_2
        );

        return Stream.of(
                Arguments.of("같은 월의 카드를 4장(총통) 가진 플레이어 1은 true를 반환해야 한다", PLAYER_1, chongtongHand, true),
                Arguments.of("같은 월의 카드를 4장(총통) 가진 플레이어 2는 true를 반환해야 한다", PLAYER_2, chongtongHand, true),
                Arguments.of("같은 월의 카드를 4장 미만으로 가진 플레이어는 false를 반환해야 한다", PLAYER_1, normalHand, false)
        );
    }

    @DisplayName("플레이어의 패를 보고 총통 여부를 정확히 판정해야 한다")
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("chongtongTestCases")
    void isConfusedPlayer_shouldReturnCorrectState(String caseName, Player player, List<Card> cards, boolean expectedResult) {
        Flux<Card> playerCards = Flux.fromIterable(cards);
        given(installedCardRepository.getPlayerCards(ROOM_ID, player))
                .willReturn(playerCards);

        StepVerifier.create(gameService.isConfusedPlayer(ROOM_ID, player))
                .expectNext(expectedResult)
                .verifyComplete();
    }
}
