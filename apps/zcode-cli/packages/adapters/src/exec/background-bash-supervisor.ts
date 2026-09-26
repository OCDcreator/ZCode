import type { ChildProcess, SpawnOptions, StdioOptions } from "node:child_process";
import type { ResolvedSpawnCommand } from "./execution-command.js";
import type { InternalExecutionRunOptions } from "./execution-adapter-types.js";

// 独立 Node 进程是 Bash 的父进程。app-server 被 SIGKILL 后，它仍可 wait 实际退出并
// 写入 session/task journal；只用 PID 或 stdout 文件无法恢复 Bash 的退出状态。
const SUPERVISOR_SOURCE = String.raw`
const { spawn } = require('node:child_process');
const { link, mkdir, open, readFile, unlink } = require('node:fs/promises');
const { dirname, join } = require('node:path');
const readConfiguration = () => new Promise((resolve, reject) => {
  process.once('message', message => {
    if (message?.type !== 'launch' || typeof message.configuration !== 'string') return reject(Error('Supervisor configuration missing'));
    if (message.configuration.length > 1048576) return reject(Error('Supervisor configuration too large'));
    try { resolve(JSON.parse(message.configuration)); } catch (error) { reject(error); }
  });
});
(async () => {
const input = await readConfiguration();
const target = join(input.journalRoot, input.sessionId, input.taskId + '.json');
const terminal = target + '.terminal';
let stopping = false;
let stopReason;
let child;
let escalation;
const signalChildTree = signal => {
  if (!child || child.exitCode !== null) return;
  if (process.platform === 'win32') {
    spawn('taskkill', ['/pid', String(child.pid), '/T', '/F'], { stdio: 'ignore', windowsHide: true });
    return;
  }
  try { process.kill(-child.pid, signal); } catch { child.kill(signal); }
};
const stop = reason => {
  if (stopping) return;
  stopping = true;
  stopReason = reason === 'timeout' ? 'timed_out' : reason === 'output_limit' ? 'failed' : 'cancelled';
  signalChildTree('SIGTERM');
  escalation = setTimeout(() => signalChildTree('SIGKILL'), 1500);
};
process.on('SIGTERM', () => stop('cancelled'));
process.on('SIGINT', () => stop('cancelled'));
// 显式取消才停 Bash。owner 被 SIGKILL 时 IPC 自动断开，但监督者继续 wait。
// 曾用 fs.ReadStream 读继承控制管道，在 macOS 的阻塞 worker 上挂住退出。
process.on('message', message => {
  if (message?.type !== 'stop') return;
  if (message.reason === 'timeout' || message.reason === 'output_limit' || message.reason === 'cancelled') stop(message.reason);
});
const write = async (status, code) => {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    let current;
    try { current = JSON.parse(await readFile(target, 'utf8')); }
    catch (error) {
      if (error.code !== 'ENOENT') throw error;
      await new Promise(resolve => setTimeout(resolve, 20));
      continue;
    }
    if (current.sessionId !== input.sessionId || current.taskId !== input.taskId || current.ownerPid !== input.ownerPid || current.toolCallId !== input.toolCallId) throw Error('Task journal identity mismatch');
    if (current.status !== 'running') {
      if (current.status === status && current.exitCode === code) return;
      throw Error('Terminal background task cannot change outcome');
    }
    const updated = { ...current, status, updatedAt: Date.now(), completedAt: Date.now(), ...(code === null ? {} : { exitCode: code }) };
    await mkdir(dirname(target), { recursive: true });
    const temporary = terminal + '.' + process.pid + '.tmp';
    const file = await open(temporary, 'wx', 0o600);
    try {
      try { await file.writeFile(JSON.stringify(updated)); await file.sync(); }
      finally { await file.close(); }
      // rename 会覆盖并发终态；硬链接以原子 create-if-absent 发布。
      try { await link(temporary, terminal); }
      catch (error) {
        if (error.code !== 'EEXIST') throw error;
        const winner = JSON.parse(await readFile(terminal, 'utf8'));
        if (winner.sessionId !== input.sessionId || winner.taskId !== input.taskId || winner.status !== status || winner.exitCode !== code) throw Error('Terminal background task cannot change outcome');
      }
    } finally { await unlink(temporary); }
    return;
  }
  throw Error('Task launch record missing');
};
try {
  child = spawn(input.file, input.args, { cwd: input.cwd, env: input.env, shell: input.shell, stdio: 'inherit', detached: process.platform !== 'win32', windowsHide: true });
  child.once('spawn', () => { if (process.connected) process.send?.({ type: 'ready' }); });
  child.once('error', async () => { try { await write('spawn_error', null); } finally { process.disconnect?.(); process.exitCode = 1; } });
  child.once('exit', async (code, signal) => {
    if (escalation) clearTimeout(escalation);
    const status = stopping ? stopReason : code === 0 ? 'completed' : 'failed';
    try { await write(status, code); }
    finally { process.disconnect?.(); process.exitCode = signal ? 1 : (code ?? 1); }
  });
} catch { await write('spawn_error', null); process.disconnect?.(); process.exitCode = 1; }
})().catch(() => { process.disconnect?.(); process.exitCode = 1; });
`;

export function superviseBackgroundBash(
  command: ResolvedSpawnCommand,
  options: SpawnOptions,
  task: NonNullable<InternalExecutionRunOptions["supervisedBackgroundTask"]>,
): { file: string; args: string[]; options: SpawnOptions; configuration: string } {
  const stdio = Array.isArray(options.stdio) ? options.stdio : ["pipe", "pipe", "pipe"];
  return {
    file: process.execPath,
    args: ["-e", SUPERVISOR_SOURCE],
    configuration: JSON.stringify({
      file: command.file,
      args: command.args,
      cwd: options.cwd,
      shell: command.shell,
      env: options.env,
      journalRoot: task.journalRoot,
      sessionId: task.sessionId,
      taskId: task.taskId,
      toolCallId: task.toolCallId,
      ownerPid: process.pid,
    }),
    options: {
      ...options,
      // Windows 宿主结束 app-server 时，非 detached 子进程随父进程树退出；
      // 监督者必须独立存活到 Bash 的实际 exit 后才能持久写终态。
      detached: true,
      shell: false,
      stdio: [...stdio, "ipc"] as StdioOptions,
      env: { ...options.env, ELECTRON_RUN_AS_NODE: "1" },
    },
  };
}

export function waitForBackgroundBashSupervisorReady(child: ChildProcess): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timeout);
      child.off("message", onMessage);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    const fail = () => { cleanup(); reject(new Error("Background Bash supervisor failed to start")); };
    const onMessage = (message: unknown) => {
      if (!message || typeof message !== "object" || (message as { type?: unknown }).type !== "ready") return;
      cleanup();
      resolve();
    };
    const onError = () => fail();
    const onExit = () => fail();
    const timeout = setTimeout(fail, 5_000);
    child.on("message", onMessage);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}
