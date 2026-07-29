-- durable 커맨드 로그 스키마 (game.log.store=postgres)
-- game_id = 게임 세대 식별자 — 같은 방의 게임 재시작 시 seq가 1부터 재시작하므로 (room_id, seq)만으론 유일하지 않다
-- FK 없음: 쓰기 hot path의 검증 비용 배제. 물리 삭제는 파티션 drop 정책으로 — 여기선 완료 표시까지만

CREATE TABLE IF NOT EXISTS game_generation (
    game_id   BIGSERIAL PRIMARY KEY,
    room_id   BIGINT  NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE
);

-- 현재 세대 조회(max game_id) + 완주 게임 정리 스캔용
CREATE INDEX IF NOT EXISTS idx_game_generation_room ON game_generation (room_id, game_id DESC);

CREATE TABLE IF NOT EXISTS game_log (
    game_id    BIGINT NOT NULL,
    seq        BIGINT NOT NULL,
    room_id    BIGINT NOT NULL,
    type       TEXT   NOT NULL,
    player     TEXT,
    card_index INT    NOT NULL,
    go         BOOLEAN NOT NULL,
    deck       TEXT,
    prev_phase TEXT,
    next_phase TEXT,
    PRIMARY KEY (game_id, seq)
);

CREATE TABLE IF NOT EXISTS game_snapshot (
    game_id BIGINT NOT NULL,
    seq     BIGINT NOT NULL,
    room_id BIGINT NOT NULL,
    state   JSONB  NOT NULL,
    PRIMARY KEY (game_id, seq)
);
