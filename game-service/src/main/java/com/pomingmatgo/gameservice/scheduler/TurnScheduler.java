package com.pomingmatgo.gameservice.scheduler;

import com.pomingmatgo.gameservice.domain.GamePhase;
import com.pomingmatgo.gameservice.domain.Player;

// TurnFlowService가 AutoPlayScheduler를 직접 주입받으면 DI cycle이 생기므로 호출부가 파라미터로 전달한다
public interface TurnScheduler {

    /** expectedPhase와 발사 시점의 실제 phase가 다르면 낡은 타이머로 보고 아무것도 하지 않는다 */
    void scheduleAutoPlay(long roomId, int round, int currentTurn, Player currentPlayer, long deadlineNanos, GamePhase expectedPhase);

    void cancelAutoPlay(long roomId);

    /** 재접속 스냅샷 표시용 근사값 — 타이머가 없으면 턴 제한 전체를 반환 */
    long getRemainingTurnMillis(long roomId);
}
