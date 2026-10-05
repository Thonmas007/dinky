const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ts = require('typescript');

// 在不连接真实任务的情况下验证模型订阅、HTTP 补偿与迟到响应的约束。
function setup() {
  const effects = [];
  const pending = [];
  const intervals = new Map();
  const sockets = [];
  let nextId = 0;
  class Socket {
    static OPEN = 1;
    static CLOSED = 3;
    readyState = 1;
    constructor(url) {
      this.url = url;
      sockets.push(this);
    }
    send() {}
    close() {
      this.readyState = 3;
    }
  }
  const source = fs.readFileSync(
    path.join(__dirname, '../src/models/UseWebSocketModel.tsx'),
    'utf8',
  );
  const module = { exports: {} };
  vm.runInNewContext(
    ts.transpileModule(source, {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
    }).outputText,
    {
      module,
      exports: module.exports,
      require(name) {
        if (name === 'react')
          return {
            useRef: (value) => ({ current: value }),
            useEffect: (effect) => effects.push(effect),
          };
        if (name === '@umijs/max')
          return {
            request: () => new Promise((resolve, reject) => pending.push({ resolve, reject })),
          };
        if (name === 'uuid') return { v4: () => String(++nextId) };
        if (name === '@/services/constants') return { TOKEN_KEY: 'token' };
        if (name === '@/utils/messages')
          return {
            ErrorMessage: (error) => {
              throw error;
            },
          };
        throw new Error(name);
      },
      window: { location: { protocol: 'https:', host: 'example.test' } },
      localStorage: { getItem: () => '{"tokenValue":"test"}' },
      WebSocket: Socket,
      Date,
      setInterval: (callback) => {
        const id = ++nextId;
        intervals.set(id, callback);
        return id;
      },
      clearInterval: (id) => intervals.delete(id),
    },
  );
  const model = module.exports.default();
  const cleanups = effects.map((effect) => effect());
  const values = [];
  const subscribe = (target = values) =>
    model.subscribeTopic('TASK_RUN_INSTANCE', ['RunningTaskId'], (data) =>
      target.push(Array.from(data.data.RunningTaskId)),
    );
  const push = (ids) => {
    sockets.at(-1).onopen();
    sockets.at(-1).onmessage({
      data: JSON.stringify({
        topic: 'TASK_RUN_INSTANCE',
        data: { RunningTaskId: ids },
      }),
    });
  };
  return { model, pending, intervals, sockets, cleanups, values, subscribe, push };
}

const settle = () => new Promise((resolve) => setImmediate(resolve));
const success = (ids) => ({ success: true, data: { RunningTaskId: ids } });

test('HTTP snapshot restores icons when the task topic is missing', async () => {
  const state = setup();
  state.subscribe();
  state.pending[0].resolve(success([7, 12]));
  await settle();
  assert.deepEqual(state.values, [[7, 12]]);
  assert.equal(state.sockets[0].url, 'wss://example.test/api/ws/global/test');
});

test('late HTTP response cannot overwrite newer WebSocket state', async () => {
  const state = setup();
  state.subscribe();
  state.push([7]);
  state.pending[0].resolve(success([]));
  await settle();
  assert.deepEqual(state.values, [[7]]);
});

test('new subscriber receives the existing snapshot immediately', async () => {
  const state = setup();
  state.subscribe();
  state.pending[0].resolve(success([12]));
  await settle();
  const second = [];
  state.subscribe(second);
  assert.deepEqual(second, [[12]]);
});

test('empty running list clears icons but failed requests do not', async () => {
  const state = setup();
  state.subscribe();
  state.push([7]);
  state.pending[0].reject(new Error('offline'));
  await settle();
  assert.deepEqual(state.values, [[7]]);
  for (const tick of state.intervals.values()) tick();
  state.pending[1].resolve(success([]));
  await settle();
  assert.deepEqual(state.values, [[7], []]);
});

test('unsubscribe and model disposal stop updates and timers', async () => {
  const state = setup();
  const unsubscribe = state.subscribe();
  unsubscribe();
  state.pending[0].resolve(success([7]));
  await settle();
  assert.deepEqual(state.values, []);
  state.cleanups.forEach((cleanup) => cleanup());
  assert.equal(state.intervals.size, 0);
  assert.equal(state.sockets.at(-1).readyState, 3);
});

test('response received after model disposal is ignored', async () => {
  const state = setup();
  state.subscribe();
  state.cleanups.forEach((cleanup) => cleanup());
  state.pending[0].resolve(success([7]));
  await settle();
  assert.deepEqual(state.values, []);
});
