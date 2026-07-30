package com.pomingmatgo.gameservice.domain.repository;

import com.pomingmatgo.gameservice.domain.gamelog.GameCommandType;
import com.pomingmatgo.gameservice.domain.gamelog.GameLogRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// durability 직교 축 — 프로파일(in-memory/redis)이 아니라 game.log.store 속성으로 선택한다 (매트릭스 폭발 방지)
public interface GameLogRepository {
    /** batch는 seq 오름차순. 방 단위 직렬 호출은 GameCommandLog의 ordered writer가 보장한다 */
    Mono<Void> append(long roomId, List<GameLogRecord> batch);

    /** 여러 방이 섞인 배치를 저장한다. batch는 전역 발행 순서라 방별 seq 오름차순을 내포한다 */
    default Mono<Void> appendAll(List<GameLogRecord> batch) {
        return Flux.fromIterable(segmentByGeneration(batch))
                .concatMap(segment -> append(segment.get(0).roomId(), segment))
                .then();
    }

    /** 방별로 묶되 DECK_INIT(새 세대 시작)에서 세그먼트를 끊는다 — 한 세그먼트 = 한 방 × 한 세대 */
    static List<List<GameLogRecord>> segmentByGeneration(List<GameLogRecord> batch) {
        List<List<GameLogRecord>> segments = new ArrayList<>();
        Map<Long, List<GameLogRecord>> openSegments = new HashMap<>();
        for (GameLogRecord record : batch) {
            List<GameLogRecord> segment = openSegments.get(record.roomId());
            if (segment == null || record.type() == GameCommandType.DECK_INIT) {
                segment = new ArrayList<>();
                segments.add(segment);
                openSegments.put(record.roomId(), segment);
            }
            segment.add(record);
        }
        return segments;
    }

    Flux<GameLogRecord> findAllFromSeq(long roomId, long fromSeq);

    /** cleanup ≠ delete — 게임 종료는 완료 표시만, 물리 삭제는 저장소 정책(파티션 drop)에 맡긴다 */
    Mono<Void> markCompleted(long roomId);

    /** 인수 스캔의 복구 대상 판정 — crash와 clean end의 구분은 lease가 아니라 이 표시다. 세대가 없으면 empty */
    default Mono<Boolean> latestGenerationCompleted(long roomId) {
        return Mono.empty();
    }

    /** false면 로그 경로 전체를 무비용 통과 — no-op 기본값에서 부하 기준선이 재현돼야 한다(직교성 검증) */
    default boolean enabled() {
        return true;
    }
}
