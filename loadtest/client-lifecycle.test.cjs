// 실행: node --test --test-isolation=none loadtest/client-lifecycle.test.cjs (서버·k6·외부 패키지 불필요)
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const vm = require('node:vm');

function harness(script, httpStatus) {
    const aborts = [], requests = [];
    const sockets = [], timers = new Map(), checks = [], metrics = new Map();
    let nextTimer = 0;
    class WebSocket {
        constructor() { this.sent = []; this.closes = 0; sockets.push(this); }
        send(data) {
            if (this.sendError) throw new Error('send failed');
            this.sent.push(JSON.parse(data));
        }
        close() {
            this.closes++;
            if (this.connecting) throw new Error('still connecting');
            if (this.onclose) this.onclose();
        }
        message(data) { this.onmessage({ data: JSON.stringify(data) }); }
    }
    class Metric {
        constructor(name) { this.name = name; }
        add(value) { metrics.set(this.name, (metrics.get(this.name) || 0) + value); }
    }
    const context = vm.createContext({
        WebSocket, Trend: Metric, Counter: Metric, vu: { idInTest: 1 }, __ENV: {}, executionTest: { abort(reason) { aborts.push(reason); } },
        http: { post: (url) => { requests.push(url); return { status: httpStatus || (url.endsWith('/join') ? 200 : 201) }; } },
        check: (value, predicates) => {
            const passed = Object.values(predicates).every((predicate) => predicate(value));
            checks.push(passed);
            return passed;
        },
        sleep() {}, console: { log() {}, warn() {}, error() {} },
        setTimeout(callback, delay) {
            const id = ++nextTimer;
            timers.set(id, { callback: () => { timers.delete(id); callback(); }, delay });
            return id;
        },
        clearTimeout(id) { timers.delete(id); },
    });
    const helper = readFileSync(join(__dirname, 'room-run.js'), 'utf8').replace(/export function/g, 'function');
    const source = readFileSync(join(__dirname, '..', script), 'utf8')
        .replace(/^import .*;\r?\n/gm, '')
        .replace('export const options', 'const options')
        .replace('export default async function ()', 'async function runScript()');
    vm.runInContext(`${helper}\n${source}`, context);
    const completion = vm.runInContext('runScript()', context);
    return { sockets, timers, checks, metrics, completion, aborts, requests };
}

for (const script of ['gostop-test.js', 'gostop-afk-test.js']) {
    for (const playerIndex of [0, 1]) {
        test(`${script}: P${playerIndex + 1} TRY_AGAIN terminates both players and queued actions`, async () => {
            const h = harness(script);
            h.sockets.forEach((ws) => ws.onopen());
            h.sockets.forEach((ws, i) => {
                ws.message({ status: 'CONNECT' });
                ws.message({ status: 'START' });
                ws.message({ status: 'ANNOUNCE_TURN_INFORMATION', curPlayer: `PLAYER_${i + 1}` });
                if (script === 'gostop-test.js') {
                    ws.message({ status: 'CHOOSE_FLOOR_CARD', player: `PLAYER_${i + 1}` });
                    ws.message({ status: 'GO_STOP_CHOICE', player: `PLAYER_${i + 1}` });
                    ws.message({ status: 'GAME_OVER' });
                }
            });
            const queued = [...h.timers.values()];
            const sends = h.sockets.map((ws) => ws.sent.length);
            h.sockets[playerIndex].message({ errorCode: 'TRY_AGAIN', errorMessage: 'busy' });
            await h.completion;
            assert.equal(h.timers.size, 0);
            assert.ok(h.sockets.every((ws) => ws.closes === 1));
            queued.forEach(({ callback }) => callback());
            h.sockets.forEach((ws) => {
                ws.message({ status: 'START' });
                ws.message({ errorCode: 'TRY_AGAIN' });
                ws.onerror({ message: 'late error' });
            });
            assert.deepEqual(h.sockets.map((ws) => ws.sent.length), sends);
            assert.equal(h.checks.filter((passed) => !passed).length, 1);
            if (script === 'gostop-test.js') {
                assert.equal(h.metrics.get('gostop_room_failures'), 1);
                assert.equal(h.metrics.get('gostop_server_errors'), 1);
            }
        });
    }

    for (const event of ['error', 'close', 'timeout', 'invalid-json', 'send-failure']) {
        test(`${script}: ${event} ends the room without waiting for peer`, async () => {
            const h = harness(script);
            const ws = h.sockets[0];
            if (event === 'error') ws.onerror({ message: 'broken' });
            if (event === 'close') ws.onclose();
            if (event === 'timeout') [...h.timers.values()][0].callback();
            if (event === 'invalid-json') ws.onmessage({ data: '{' });
            if (event === 'send-failure') { ws.sendError = true; ws.onopen(); }
            await h.completion;
            assert.equal(h.timers.size, 0);
            assert.ok(h.sockets.every((socket) => socket.closes === 1));
            assert.equal(h.checks.at(-1), false);
        });
    }

    test(`${script}: late open after failure closes again without CONNECT`, async () => {
        const h = harness(script);
        h.sockets[1].connecting = true;
        h.sockets[0].message({ errorCode: 'TRY_AGAIN' });
        await h.completion;
        h.sockets[1].connecting = false;
        h.sockets[1].onopen();
        assert.equal(h.sockets[1].closes, 2);
        assert.equal(h.sockets[1].sent.length, 0);
        assert.equal(h.timers.size, 0);
    });
}

test('AFK: both GAME_OVER messages complete successfully and clear watchdogs', async () => {
    const h = harness('gostop-afk-test.js');
    h.sockets.forEach((ws) => ws.onopen());
    h.sockets[0].message({ status: 'GAME_OVER' });
    assert.equal(h.sockets[1].closes, 0);
    h.sockets[1].message({ status: 'GAME_OVER' });
    await h.completion;
    assert.ok(h.checks.every(Boolean));
    assert.equal(h.timers.size, 0);
});

test('AFK: one GAME_OVER followed by peer error is a failure', async () => {
    const h = harness('gostop-afk-test.js');
    h.sockets[0].message({ status: 'GAME_OVER' });
    h.sockets[1].message({ errorCode: 'TRY_AGAIN' });
    await h.completion;
    assert.equal(h.checks.at(-1), false);
    assert.equal(h.timers.size, 0);
});

test('normal client: scheduled actions and post-game READY still send before failure', async () => {
    const h = harness('gostop-test.js');
    const ws = h.sockets[0];
    ws.onopen();
    h.sockets.forEach((socket, i) => socket.message({ status: 'CONNECT', player: `PLAYER_${i + 1}` }));
    ws.message({ status: 'READY', player: 'PLAYER_1' });
    h.sockets.forEach((socket) => socket.message({ status: 'START' }));
    ws.message({ status: 'LEADER_SELECTION', player: 'PLAYER_1' });
    for (const status of ['ANNOUNCE_TURN_INFORMATION', 'CHOOSE_FLOOR_CARD', 'GO_STOP_CHOICE', 'GAME_OVER']) {
        ws.message({ status, curPlayer: 'PLAYER_1', player: 'PLAYER_1' });
        if (status === 'GAME_OVER') h.sockets[1].message({ status });
        [...h.timers.values()].filter(({ delay }) => delay < 10000).forEach(({ callback }) => callback());
    }
    assert.deepEqual(ws.sent.map((request) => request.eventType.subType),
        ['CONNECT', 'READY', 'LEADER_SELECTION', 'NORMAL_SUBMIT', 'FLOOR_SELECT', 'GO_STOP_CHOICE', 'READY']);
    assert.equal(h.metrics.get('gostop_games_completed'), 1);
    assert.equal(h.metrics.get('gostop_room_failures'), 0);
    ws.onerror({ message: 'test teardown' });
    await h.completion;
    assert.equal(h.timers.size, 0);
});

for (const script of ['gostop-test.js', 'gostop-afk-test.js']) {
    test(`${script}: own CONNECT barrier and acknowledged preparation send each request once`, async () => {
        const h = harness(script);
        const [p1, p2] = h.sockets;
        h.sockets.forEach((ws) => ws.onopen());
        p2.message({ status: 'CONNECT', player: 'PLAYER_2' });
        p1.message({ status: 'CONNECT', player: 'PLAYER_2' });
        assert.equal(p1.sent.length, 1);
        assert.equal(p2.sent.length, 1);
        p1.message({ status: 'CONNECT', player: 'PLAYER_1' });
        p1.message({ status: 'CONNECT', player: 'PLAYER_1' });
        assert.equal(p1.sent.at(-1).eventType.subType, 'READY');
        assert.equal(p1.sent.length, 2);
        assert.equal(p2.sent.length, 1);
        p2.message({ status: 'READY', player: 'PLAYER_1' });
        p1.message({ status: 'READY', player: 'PLAYER_1' });
        assert.equal(p2.sent.at(-1).eventType.subType, 'READY');
        assert.equal(p2.sent.length, 2);
        p2.message({ status: 'START' });
        assert.equal(p1.sent.length, 2);
        p1.message({ status: 'START' });
        p1.message({ status: 'START' });
        assert.equal(p1.sent.at(-1).eventType.subType, 'LEADER_SELECTION');
        assert.equal(p2.sent.length, 2);
        h.sockets.forEach((ws) => ws.message({ status: 'LEADER_SELECTION', player: 'PLAYER_1' }));
        assert.equal(p1.sent.length, 3);
        assert.equal(p2.sent.length, 3);
        assert.equal(p2.sent.at(-1).data.cardIndex, '2');
        p1.onerror({ message: 'test teardown' });
        await h.completion;
    });
}

test('normal client: waits for both GAME_OVER before one restart and resets preparation', async () => {
    const h = harness('gostop-test.js');
    const [p1, p2] = h.sockets;
    p1.message({ status: 'GAME_OVER' });
    assert.equal([...h.timers.values()].filter((timer) => timer.delay === 1000).length, 0);
    p2.message({ status: 'GAME_OVER' });
    p1.message({ status: 'GAME_OVER' });
    const restart = [...h.timers.values()].filter((timer) => timer.delay === 1000);
    assert.equal(restart.length, 1);
    restart[0].callback();
    assert.equal(p1.sent.at(-1).eventType.subType, 'READY');
    assert.equal(p2.sent.length, 0);
    p2.message({ status: 'READY', player: 'PLAYER_1' });
    h.sockets.forEach((ws) => ws.message({ status: 'START' }));
    p2.message({ status: 'LEADER_SELECTION', player: 'PLAYER_1' });
    assert.deepEqual(p2.sent.map((req) => req.eventType.subType), ['READY', 'LEADER_SELECTION']);
    p1.onerror({ message: 'test teardown' });
    await h.completion;
    assert.equal(h.aborts.length, 1);
});

for (const status of [409, 500]) {
    test(`normal client: HTTP ${status} aborts before sockets or another room creation`, async () => {
        const h = harness('gostop-test.js', status);
        await h.completion;
        assert.equal(h.aborts.length, 1);
        assert.equal(h.sockets.length, 0);
        assert.equal(h.requests.length, 1);
        assert.equal(h.checks.at(-1), false);
    });
}

test('normal client: preparation wait timeout aborts after closing both players', async () => {
    const h = harness('gostop-test.js');
    h.sockets[0].message({ status: 'CONNECT', player: 'PLAYER_1' });
    [...h.timers.values()].find((timer) => timer.delay === 10000).callback();
    await h.completion;
    assert.equal(h.aborts.length, 1);
    assert.equal(h.metrics.get('gostop_client_timeouts'), 1);
    assert.ok(h.sockets.every((ws) => ws.closes === 1));
});
