package com.pomingmatgo.gameservice.global.session;

import com.pomingmatgo.gameservice.domain.Player;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.web.reactive.socket.WebSocketSession;

@Setter
@Getter
@NoArgsConstructor
public class RoomSessionData {
    private Long player1Id;
    private WebSocketSession player1Session;
    private Long player2Id;
    private WebSocketSession player2Session;

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
