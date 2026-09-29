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
