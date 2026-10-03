// 한 플레이어의 실패로 방 단위 실행을 종료한다. 요청 상관관계가 없어 자동 재시도하지 않는다.
export function createRoomRun(onFailure) {
    let failed = false;
    const players = [];

    return {
        get failed() { return failed; },
        fail(reason) {
            if (failed) return;
            failed = true;
            onFailure(reason);
            players.forEach((player) => player.finish());
        },
        addPlayer(ws, resolve) {
            let stopped = false;
            const timers = new Set();
            const player = {
                get stopped() { return stopped; },
                schedule(callback, delay) {
                    if (stopped) return null;
                    const timer = setTimeout(() => {
                        timers.delete(timer);
                        if (!stopped) callback();
                    }, delay);
                    timers.add(timer);
                    return timer;
                },
                cancel(timer) {
                    if (timer === null) return;
                    clearTimeout(timer);
                    timers.delete(timer);
                },
                finish() {
                    const wasStopped = stopped;
                    stopped = true;
                    timers.forEach((timer) => clearTimeout(timer));
                    timers.clear();
                    // CONNECTING 중 close가 거부돼도 늦은 onopen에서 다시 닫는다.
                    try { ws.close(); } catch (_) { /* 종료/연결 중인 소켓 */ }
                    if (!wasStopped) resolve();
                },
            };
            players.push(player);
            if (failed) player.finish();
            return player;
        },
    };
}

// 같은 VU의 두 연결을 서버 안내로 조율한다. 실패한 요청을 재전송하지 않는다.
export function createPreparationFlow(sendReady, sendSelection, onGameOver = () => {}) {
    const connected = new Set();
    let started = new Set();
    let ended = new Set();
    let readySent = new Set();
    let selectionSent = new Set();

    function ready(player) {
        if (readySent.has(player)) return;
        readySent.add(player);
        sendReady(player);
    }

    return {
        restart() {
            if (ended.size !== 2) return;
            started = new Set();
            ended = new Set();
            readySent = new Set();
            selectionSent = new Set();
            ready('PLAYER_1');
        },
        onMessage(receiver, res) {
            const status = res.status || (res.eventType && res.eventType.subType);
            const actor = res.player || (res.data && res.data.player);
            if (status === 'CONNECT' && actor === receiver) {
                connected.add(receiver);
                if (connected.size === 2) ready('PLAYER_1');
            }
            if (status === 'READY' && actor === 'PLAYER_1' && readySent.has(actor)) {
                ready('PLAYER_2');
            }
            if (status === 'START') {
                started.add(receiver);
                if (started.size === 2 && !selectionSent.has('PLAYER_1')) {
                    selectionSent.add('PLAYER_1');
                    sendSelection('PLAYER_1');
                }
            }
            if (status === 'LEADER_SELECTION' && actor === 'PLAYER_1'
                    && selectionSent.has(actor) && !selectionSent.has('PLAYER_2')) {
                selectionSent.add('PLAYER_2');
                sendSelection('PLAYER_2');
            }
            if (status === 'GAME_OVER' && !ended.has(receiver)) {
                ended.add(receiver);
                if (ended.size === 2) onGameOver();
            }
        },
    };
}
