package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;

/**
 * 턴 완료 후처리(TurnFlowService)가 자동플레이 타이머를 조작하기 위한 최소 인터페이스.
 *
 * TurnFlowService가 AutoPlayScheduler를 직접 주입받으면
 * AutoPlayScheduler → TurnFlowService → AutoPlayScheduler DI cycle이 생기므로,
 * 호출부(WsGameHandler / AutoPlayScheduler 자신)가 파라미터로 전달한다.
 */
public interface TurnScheduler {

    /**
     * @param expectedPhase 타이머 발사 시점에 기대하는 게임 phase.
     *                      IN_PROGRESS → 카드 자동 제출, AWAITING_FLOOR_CARD_CHOICE → 바닥 카드 자동 선택,
     *                      AWAITING_GO_STOP_CHOICE → 자동 STOP.
     *                      발사 시점의 실제 phase가 다르면 낡은 타이머로 판단하고 아무것도 하지 않는다.
     */
    void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos, GamePhase expectedPhase);

    void cancelAutoPlay(long roomId);

    /** 대기 중인 타이머 기준 남은 턴 시간(ms). 재접속 스냅샷 표시용 근사값 — 타이머가 없으면 턴 제한 전체를 반환 */
    long getRemainingTurnMillis(long roomId);
}
