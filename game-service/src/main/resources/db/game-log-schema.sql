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
    -- DECK_INIT 전용(세대 출생 기록) — 스냅샷 없는(라운드 1) 복구가 초기 GameState를 로그만으로 복원하는 근거
    user1_id       BIGINT,
    user2_id       BIGINT,
    leading_player INT NOT NULL DEFAULT 0,
    PRIMARY KEY (game_id, seq)
);

-- 기존 DB 마이그레이션 (CREATE IF NOT EXISTS는 기존 테이블에 컬럼을 더하지 않는다)
ALTER TABLE game_log ADD COLUMN IF NOT EXISTS user1_id BIGINT;
ALTER TABLE game_log ADD COLUMN IF NOT EXISTS user2_id BIGINT;
ALTER TABLE game_log ADD COLUMN IF NOT EXISTS leading_player INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS game_snapshot (
    game_id BIGINT NOT NULL,
    seq     BIGINT NOT NULL,
    room_id BIGINT NOT NULL,
    state   JSONB  NOT NULL,
    PRIMARY KEY (game_id, seq)
);

-- 방 소유권 lease (game.lease.store=postgres) — 만료 판정 시계는 DB now()로 단일화 (노드 간 시계 오차 배제)
-- 정상 해제도 행을 지우지 않고 만료 처리만 한다 — fencing_token 단조 증가 보존 (좀비의 낡은 토큰이 재사용 방에서 유효해지는 것 방지)
CREATE TABLE IF NOT EXISTS room_lease (
    room_id                    BIGINT PRIMARY KEY,
    owner_instance             TEXT        NOT NULL,
    fencing_token              BIGINT      NOT NULL,
    expires_at                 TIMESTAMPTZ NOT NULL,
    turn_deadline_epoch_millis BIGINT
);
