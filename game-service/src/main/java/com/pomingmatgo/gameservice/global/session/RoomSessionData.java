package com.pomingmatgo.gameservice.global.session;

import com.pomingmatgo.gameservice.domain.Player;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Sinks;

@Setter
@Getter
@NoArgsConstructor
public class RoomSessionData {
    private Long player1Id;
    private WebSocketSession player1Session;
    private Long player2Id;
    private WebSocketSession player2Session;

    public Long getUserIdByPlayerNum(int playerNum) {
        if (playerNum == 1) return player1Id;
        if (playerNum == 2) return player2Id;
        return null;
    }

    public Integer getPlayerNumByUserId(long userId) {
        if (userId == player1Id) return 1;
        if (userId == player2Id) return 2;
        return null;
    }

    public void addPlayer(Player player, long userId, WebSocketSession session) {
        if (player == Player.PLAYER_1) {
            this.player1Id = userId;
            this.player1Session = session;
        } else if (player == Player.PLAYER_2) {
            this.player2Id = userId;
            this.player2Session = session;
        }
    }
}
