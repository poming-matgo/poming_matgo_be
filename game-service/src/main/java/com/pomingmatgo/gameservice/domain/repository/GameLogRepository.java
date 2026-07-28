package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

// durability 직교 축 — 프로파일(in-memory/redis)이 아니라 game.log.store 속성으로 선택한다 (매트릭스 폭발 방지)
public interface GameLogRepository {
    /** batch는 seq 오름차순. 방 단위 직렬 호출은 GameCommandLog의 ordered writer가 보장한다 */
    Mono<Void> append(long roomId, List<GameLogRecord> batch);

    Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq);

    /** cleanup ≠ delete — 게임 종료는 완료 표시만, 물리 삭제는 저장소 정책(파티션 drop)에 맡긴다 */
    Mono<Void> markCompleted(long roomId);

    /** false면 로그 경로 전체를 무비용 통과 — no-op 기본값에서 부하 기준선이 재현돼야 한다(직교성 검증) */
    default boolean enabled() {
        return true;
    }
}
