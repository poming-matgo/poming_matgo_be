import http from 'k6/http';
import { check, sleep } from 'k6';
import { vu } from 'k6/execution';
import { WebSocket } from 'k6/websockets';

export const options = {
    stages: [
        { duration: '2m', target: 5000 },
        { duration: '7m', target: 5000 }, // 7분 동안 1,000 VU 유지
        { duration: '1m', target: 0 }, // 1분 동안 서서히 종료
    ],
};

const BASE_HTTP_URL = 'http://127.0.0.1:8084';
const BASE_WS_URL = 'ws://127.0.0.1:8084/gostop';

export default async function () {
    const roomId = vu.idInTest;
    const p1_userId = (roomId * 2) - 1;
    const p2_userId = (roomId * 2);
    
    const headers = { 'Content-Type': 'application/json' };

    // 1. 방 생성
    let createRes = http.post(`${BASE_HTTP_URL}/room`, JSON.stringify({ roomId: roomId.toString() }), { headers });
    let created = check(createRes, { 'Room created': (r) => r.status === 201 });
    
    if (!created) {
        console.error(`[Room:${roomId}] 방 생성 실패: ${createRes.status}`);
        return; 
    }

    // [중요] 방 생성 후 백엔드가 방을 메모리에 올릴 시간을 확보
    sleep(0.5); 

    // 2. 방 입장
    let join1 = http.post(`${BASE_HTTP_URL}/room/join`, JSON.stringify({ roomId: roomId.toString(), userId: p1_userId.toString() }), { headers });
    let join2 = http.post(`${BASE_HTTP_URL}/room/join`, JSON.stringify({ roomId: roomId.toString(), userId: p2_userId.toString() }), { headers });

    let joined = check(join1, { 'P1 Joined': (r) => r.status === 200 }) && 
                 check(join2, { 'P2 Joined': (r) => r.status === 200 });

    if (!joined) {
        console.error(`[Room:${roomId}] 유저 입장 실패. P1:${join1.status}, P2:${join2.status}`);
        return; 
    }

    // 3. 웹소켓 연결 및 게임 로직
    function connectPlayer(userId, playerType) {
        return new Promise((resolve) => {
            const ws = new WebSocket(BASE_WS_URL);
            const logPrefix = `[VU:${vu.idInTest} | Room:${roomId} | ${playerType}]`;

            let activityTimer = null;
            let lastSentReq = "소켓 연결 시도 중"; 

            // 💡 [수정됨] 무슨 요청(Req)을 보냈는지 추적하는 타이머 함수
            function startTimeoutTimer(reqDescription) {
                lastSentReq = reqDescription; 
                if (activityTimer) clearTimeout(activityTimer);
                
                activityTimer = setTimeout(() => {
                    console.error(`${logPrefix} 🚨 10초 타임아웃! 멈추기 전 마지막으로 보낸 요청(Req): [${lastSentReq}]`);
                }, 10000); 
            }

            // 💡 [수정됨] 요청 전송과 타이머 시작을 하나로 묶은 함수 (이 함수로만 send 수행)
            function sendReq(payload, description) {
                startTimeoutTimer(description);
                ws.send(JSON.stringify(payload));
            }

            ws.onopen = () => {
                sendReq(
                    { eventType: { type: "JOIN_ROOM", subType: "CONNECT" }, data: { userId: userId, roomId: roomId } },
                    "JOIN_ROOM_CONNECT (방 입장)"
                );
            };

            ws.onmessage = (e) => {
                try {
                    let res = JSON.parse(e.data);

                    // 💡 [수정됨] 서버 에러(errorCode) 분리 및 출력
                    if (res.errorCode) {
                        console.error(`${logPrefix} ❌ [서버 에러 응답] Code: ${res.errorCode}, Message: ${res.errorMessage}`);
                        if (activityTimer) clearTimeout(activityTimer); // 에러를 받았으므로 타임아웃 해제
                        return; // 로직 중단
                    }

                    // 정상 메시지를 받았으므로 타임아웃 해제
                    if (activityTimer) clearTimeout(activityTimer);

                    let status = res.status || (res.eventType && res.eventType.subType);
                    if (!status) return;

                    switch (status) {
                        case 'CONNECT':
                            sendReq({ eventType: { type: "ROOM", subType: "READY" } }, "ROOM_READY (준비)");
                            break;

                        case 'START':
                            let cardIdx = (playerType === "PLAYER_1") ? "1" : "2";
                            sendReq(
                                { eventType: { type: "PREGAME", subType: "LEADER_SELECTION" }, data: { cardIndex: cardIdx } },
                                `PREGAME_LEADER_SELECTION (선택 카드: ${cardIdx})`
                            );
                            break;

                        case 'ANNOUNCE_TURN_INFORMATION':
                            let currentPlayer = (res.data && res.data.curPlayer) || res.curPlayer;
                            if (currentPlayer === playerType) {
                                setTimeout(() => {
                                    sendReq(
                                        { eventType: { type: "GAME", subType: "NORMAL_SUBMIT" }, data: { cardIndex: "0" } },
                                        "GAME_NORMAL_SUBMIT (일반 카드 0번 제출)"
                                    );
                                }, 500);
                            }
                            break;
                        
                        case 'CHOOSE_FLOOR_CARD':
                            let targetPlayer = res.player || (res.data && res.data.player);
                            if (targetPlayer === playerType) {
                                setTimeout(() => {
                                    sendReq(
                                        { eventType: { type: "GAME", subType: "FLOOR_SELECT" }, data: { cardIndex: "0" } },
                                        "GAME_FLOOR_SELECT (바닥 카드 0번 선택)"
                                    );
                                }, 500);
                            }
                            break;

                        case 'GO_STOP_CHOICE':
                            let decider = res.player || (res.data && res.data.player);
                            if (decider === playerType) {
                                let randomGo = Math.random() < 0.5;
                                setTimeout(() => {
                                    sendReq(
                                        { eventType: { type: "GAME", subType: "GO_STOP_CHOICE" }, data: { go: randomGo } },
                                        `GAME_GO_STOP_CHOICE (${randomGo ? 'GO' : 'STOP'} 선택)`
                                    );
                                }, 500);
                            }
                            break;

                        case 'GAME_OVER':
                            setTimeout(() => {
                                sendReq({ eventType: { type: "ROOM", subType: "READY" } }, "ROOM_READY (게임 종료 후 재준비)");
                            }, 1000);
                            break;
                    }
                } catch (err) {
                    console.error(`${logPrefix} 메시지 처리 중 에러 발생: ${err.message}, 원본 데이터: ${e.data}`);
                }
            };

            ws.onerror = (e) => {
                console.error(`${logPrefix} 🔴 WS 에러: ${e.message}`);
                resolve();
            };

            ws.onclose = () => {
                resolve();
            };
        });
    }

    await Promise.all([
        connectPlayer(p1_userId, "PLAYER_1"),
        connectPlayer(p2_userId, "PLAYER_2")
    ]);
}