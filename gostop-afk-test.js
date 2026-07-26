// AFK(완전 방치) 기능 테스트: 두 플레이어가 카드 제출/바닥 카드 선택/고스톱 선택을 전혀 하지 않아도
// 서버 자동플레이(제출 + 바닥 카드 자동 선택 + 고/스톱 자동 STOP)만으로 게임이 정지 없이 완주되는지 검증한다.
//
// 실행: k6 run gostop-afk-test.js  (서버가 in-memory 프로파일로 떠 있어야 함)
// 소요: 최대 약 8분 (20턴 × 12초 + 바닥 카드 선택 타임아웃 12초 × 발생 횟수)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { WebSocket } from 'k6/websockets';
import { Counter } from 'k6/metrics';

export const options = {
    scenarios: {
        afk_game: {
            executor: 'per-vu-iterations',
            vus: 1,
            iterations: 1,
            maxDuration: '9m',
        },
    },
};

const BASE_HTTP_URL = 'http://127.0.0.1:8084';
const BASE_WS_URL = 'ws://127.0.0.1:8084/gostop';
// 턴 자동플레이(12초) + 바닥 카드 자동 선택(12초)이 연달아 이어져도 걸리지 않는 정지 판정 한계
const STALL_LIMIT_MS = 30000;

const floorChoiceCounter = new Counter('afk_floor_choices');
const goStopChoiceCounter = new Counter('afk_go_stop_choices');
const turnCounter = new Counter('afk_turn_announcements');
const threePpeokCounter = new Counter('afk_three_ppeok_wins');

function connectPlayer(userId, playerType, roomId, result) {
    return new Promise((resolve) => {
        const ws = new WebSocket(BASE_WS_URL);
        const logPrefix = `[Room:${roomId} | ${playerType}]`;
        let watchdog = null;
        let done = false;

        function finish() {
            if (done) return;
            done = true;
            if (watchdog) clearTimeout(watchdog);
            try { ws.close(); } catch (e) { /* already closed */ }
            resolve();
        }

        function resetWatchdog() {
            if (watchdog) clearTimeout(watchdog);
            watchdog = setTimeout(() => {
                console.error(`${logPrefix} 🚨 ${STALL_LIMIT_MS / 1000}초간 진행 메시지 없음 — 게임 정지(stall)로 판단`);
                result.stalled = true;
                finish();
            }, STALL_LIMIT_MS);
        }

        ws.onopen = () => {
            resetWatchdog();
            ws.send(JSON.stringify({ eventType: { type: 'JOIN_ROOM', subType: 'CONNECT' }, data: { userId: userId, roomId: roomId } }));
        };

        ws.onmessage = (e) => {
            let res;
            try { res = JSON.parse(e.data); } catch (err) { return; }

            if (res.errorCode) {
                console.error(`${logPrefix} ❌ 서버 에러: ${res.errorCode} ${res.errorMessage || ''}`);
                return;
            }

            const status = res.status || (res.eventType && res.eventType.subType);
            if (!status) return;

            resetWatchdog();

            switch (status) {
                case 'CONNECT':
                    ws.send(JSON.stringify({ eventType: { type: 'ROOM', subType: 'READY' } }));
                    break;

                case 'START': {
                    const cardIdx = playerType === 'PLAYER_1' ? '1' : '2';
                    ws.send(JSON.stringify({ eventType: { type: 'PREGAME', subType: 'LEADER_SELECTION' }, data: { cardIndex: cardIdx } }));
                    break;
                }

                case 'ANNOUNCE_TURN_INFORMATION':
                    // AFK: 카드를 내지 않는다 → 서버 자동플레이가 12초 후 대신 제출해야 함
                    result.turns++;
                    turnCounter.add(1);
                    break;

                case 'CHOOSE_FLOOR_CARD': {
                    // AFK: 선택하지 않는다 → 서버가 12초 후 자동 선택해야 함 (이번 변경의 핵심 검증 지점)
                    const target = res.player || (res.data && res.data.player);
                    if (target === playerType) {
                        result.floorChoices++;
                        floorChoiceCounter.add(1);
                        console.log(`${logPrefix} 🃏 바닥 카드 선택 요청 수신 — 무시하고 자동 선택 대기`);
                    }
                    break;
                }

                case 'GO_STOP_CHOICE': {
                    // AFK: 선택하지 않는다 → 서버가 12초 후 자동 STOP으로 게임을 종료해야 함
                    const decider = res.player || (res.data && res.data.player);
                    if (decider === playerType) {
                        result.goStopChoices++;
                        goStopChoiceCounter.add(1);
                        console.log(`${logPrefix} 🛑 고/스톱 선택 요청 수신 — 무시하고 자동 STOP 대기`);
                    }
                    break;
                }

                case 'THREE_PPEOK': {
                    // 뻑 3회 → 7점 즉시 승리. 고/스톱 대기 없이 곧바로 GAME_OVER가 이어진다
                    const winner = res.player || (res.data && res.data.player);
                    result.threePpeokWinner = winner;
                    if (winner === playerType) {
                        threePpeokCounter.add(1);
                        console.log(`${logPrefix} 💥 세번뻑 — 7점 즉시 승리로 게임 종료`);
                    }
                    break;
                }

                case 'GAME_OVER':
                    console.log(`${logPrefix} 🏁 GAME_OVER 수신 — 완주`);
                    result.gameOver = true;
                    finish();
                    break;
            }
        };

        ws.onerror = () => finish();
        ws.onclose = () => finish();
    });
}

export default async function () {
    const roomId = 1;
    const headers = { 'Content-Type': 'application/json' };

    const createRes = http.post(`${BASE_HTTP_URL}/room`, JSON.stringify({ roomId: roomId.toString() }), { headers });
    if (!check(createRes, { '방 생성': (r) => r.status === 201 })) return;
    sleep(0.5);

    const join1 = http.post(`${BASE_HTTP_URL}/room/join`, JSON.stringify({ roomId: roomId.toString(), userId: '1' }), { headers });
    const join2 = http.post(`${BASE_HTTP_URL}/room/join`, JSON.stringify({ roomId: roomId.toString(), userId: '2' }), { headers });
    if (!check(join1, { 'P1 입장': (r) => r.status === 200 }) || !check(join2, { 'P2 입장': (r) => r.status === 200 })) return;

    const r1 = { gameOver: false, stalled: false, floorChoices: 0, goStopChoices: 0, turns: 0, threePpeokWinner: null };
    const r2 = { gameOver: false, stalled: false, floorChoices: 0, goStopChoices: 0, turns: 0, threePpeokWinner: null };

    await Promise.all([
        connectPlayer(1, 'PLAYER_1', roomId, r1),
        connectPlayer(2, 'PLAYER_2', roomId, r2),
    ]);

    check(null, {
        'AFK 게임이 정지(stall) 없이 완주': () => (r1.gameOver || r2.gameOver) && !r1.stalled && !r2.stalled,
    });

    const totalChoices = r1.floorChoices + r2.floorChoices;
    const ppeokWinner = r1.threePpeokWinner || r2.threePpeokWinner;
    const endReason = ppeokWinner ? `세번뻑 즉시 승리 (${ppeokWinner})` : '고/스톱 자동 STOP 또는 최종 라운드 종료';
    console.log(`📊 결과 — 종료 사유: ${endReason}, 턴 공지: ${r1.turns}회, 바닥 카드 선택 발생: P1=${r1.floorChoices} P2=${r2.floorChoices}, 고/스톱 선택 발생: P1=${r1.goStopChoices} P2=${r2.goStopChoices}`);
    if (totalChoices === 0) {
        // 세번뻑으로 조기 종료되면 선택 상황을 만날 기회 자체가 적다
        console.warn('⚠️ 이번 판에서는 바닥 카드 선택 상황이 발생하지 않았습니다 (덱 셔플 랜덤). 재실행을 권장합니다.');
    }
}
