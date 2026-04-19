import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { vu } from 'k6/execution';

export const options = {
    vus: 2,
    duration: '1m',
};

const BASE_HTTP_URL = 'http://127.0.0.1:8084';
const BASE_WS_URL = 'ws://127.0.0.1:8084/gostop';

export default function () {
    const userId = vu.idInTest;
    const roomId = Math.ceil(userId / 2);
    const isOdd = userId % 2 !== 0;
    const myPlayerType = isOdd ? "PLAYER_1" : "PLAYER_2";
    const logPrefix = `[VU:${userId} | Room:${roomId} | ${myPlayerType}]`;

    const headers = { 'Content-Type': 'application/json' };

    // 1. 방 생성 (홀수 유저만)
    if (isOdd) {
        console.log(`${logPrefix} Creating room...`);
        let createRes = http.post(`${BASE_HTTP_URL}/room`, JSON.stringify({ roomId: roomId.toString() }), { headers });
        check(createRes, { 'Room created successfully': (r) => r.status === 201 });
    } else {
        sleep(1); // 방 생성을 기다림
    }

    // 2. 방 입장
    console.log(`${logPrefix} Joining room...`);
    let joinRes = http.post(`${BASE_HTTP_URL}/room/join`, JSON.stringify({
        roomId: roomId.toString(),
        userId: userId.toString()
    }), { headers });

    if (check(joinRes, { 'Joined room successfully': (r) => r.status === 200 })) {
        console.log(`${logPrefix} Successfully joined HTTP room`);
    } else {
        console.error(`${logPrefix} Failed to join room: ${joinRes.status}`);
        return;
    }

    // 3. WebSocket 연결
    let wsRes = ws.connect(BASE_WS_URL, function (socket) {

        function sendEvent(type, subType, data = null) {
            let payload = { eventType: { type: type, subType: subType } };
            if (data) payload.data = data;
            console.log(`${logPrefix} >> SEND: ${type}.${subType} ${data ? JSON.stringify(data) : ''}`);
            socket.send(JSON.stringify(payload));
        }

        socket.on('open', () => {
            console.log(`${logPrefix} WebSocket Connected. Sending JOIN_ROOM...`);
            sendEvent('JOIN_ROOM', 'CONNECT', { userId: userId, roomId: roomId });
        });

        socket.on('message', (msg) => {
            let res = JSON.parse(msg);
            let status = res.status;
            console.log(`${logPrefix} << RECV: ${status}`);

            switch (status) {
                case 'CONNECT':
                    console.log(`${logPrefix} Connection confirmed. Sending READY...`);
                    sendEvent('ROOM', 'READY');
                    break;

                case 'START':
                    let cardIdx = isOdd ? "1" : "2";
                    console.log(`${logPrefix} Game started! Selecting leader with card index: ${cardIdx}`);
                    sendEvent('PREGAME', 'LEADER_SELECTION', { cardIndex: cardIdx });
                    break;

                case 'ANNOUNCE_TURN_INFORMATION':
                    let curPlayer = res.data.curPlayer;
                    if (curPlayer === myPlayerType) {
                        console.log(`${logPrefix} My turn! Submitting card...`);
                        socket.setTimeout(() => {
                            sendEvent('GAME', 'NORMAL_SUBMIT', { cardIndex: "0" });
                        }, 500);
                    } else {
                        console.log(`${logPrefix} Wait... It's ${curPlayer}'s turn.`);
                    }
                    break;

                case 'CHOOSE_FLOOR_CARD':
                    if (res.player === myPlayerType) {
                        console.log(`${logPrefix} Multiple floor cards detected. Choosing index 0...`);
                        socket.setTimeout(() => {
                            sendEvent('GAME', 'FLOOR_SELECT', { cardIndex: "0" });
                        }, 500);
                    }
                    break;

                case 'GO_STOP_CHOICE':
                    if (res.player === myPlayerType) {
                        let randomGo = Math.random() < 0.5;
                        console.log(`${logPrefix} Go or Stop? Decision: ${randomGo ? 'GO' : 'STOP'}`);
                        socket.setTimeout(() => {
                            sendEvent('GAME', 'GO_STOP_CHOICE', { go: randomGo });
                        }, 500);
                    }
                    break;

                case 'GAME_OVER':
                    console.log(`${logPrefix} Game Over! Sending READY for next round...`);
                    socket.setTimeout(() => {
                        sendEvent('ROOM', 'READY');
                    }, 1000);
                    break;

                default:
                    break;
            }
        });

        socket.on('error', (e) => {
            if (e.error() != "websocket: close sent") {
                console.error(`${logPrefix} WebSocket Error: ${e.error()}`);
            }
        });

        socket.on('close', () => {
            console.log(`${logPrefix} WebSocket Connection Closed`);
        });

        // 테스트 duration 종료 시 소켓 닫기
        socket.setTimeout(function () {
            console.log(`${logPrefix} Force closing socket after 1 minute.`);
            socket.close();
        }, 60000);
    });

    check(wsRes, { 'WebSocket connected successfully': (r) => r && r.status === 101 });
}