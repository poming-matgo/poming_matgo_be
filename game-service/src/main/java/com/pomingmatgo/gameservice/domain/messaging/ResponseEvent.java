package com.pomingmatgo.gameservice.domain.messaging;

/** 클라이언트로 나가는 응답 이벤트 타입 — 클라 프로토콜 계약이므로 이름 변경 시 클라 협의 필요. wire 표현은 name() */
public enum ResponseEvent {
    // 방/접속
    CONNECT, READY, UNREADY, START,
    RECONNECT, RECONNECT_STATE, OPPONENT_DISCONNECTED,
    // 선 선택/카드 배분
    LEADER_SELECTION, LEADER_SELECTION_RESULT, DISTRIBUTE_CARD, DISTRIBUTED_FLOOR_CARD,
    // 게임 진행
    ANNOUNCE_TURN_INFORMATION, SUBMIT_CARD, CARD_REVEALED, ACQUIRED_CARD,
    CHOOSE_FLOOR_CARD, OPPONENT_PI_CLAIMED, SCORE_UPDATE,
    PPEOK, TTADAK, JJOK,
    GO_STOP_CHOICE, OPPONENT_GO_STOP_CHOICE, GO_RESULT, GAME_OVER
}
