import { link, mkdir, open, readFile, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";

export type BackgroundTaskJournalStatus =
  | "running"
  | "completed"
  | "failed"
  | "timed_out"
  | "cancelled"
  | "spawn_error";

export interface BackgroundTaskJournalRecord {
  version: 1;
  sessionId: string;
  taskId: string;
  toolCallId: string;
  status: BackgroundTaskJournalStatus;
  startedAt: number;
  updatedAt: number;
  completedAt?: number;
  exitCode?: number;
  ownerPid: number;
}

function validId(value: string): boolean {
  return /^[a-zA-Z0-9_-]{1,120}$/.test(value);
}

function validToolCallId(value: string): boolean {
  // 工具调用 ID 只存为记录字段，模型提供商会生成 Bash:0；不能套用路径段校验。
  return /^[a-zA-Z0-9_:-]{1,120}$/.test(value);
}

async function publishImmutable(target: string, record: BackgroundTaskJournalRecord): Promise<void> {
  await mkdir(dirname(target), { recursive: true });
  const temporary = `${target}.${crypto.randomUUID()}.tmp`;
  const file = await open(temporary, "wx", 0o600);
  try {
    try {
      await file.writeFile(JSON.stringify(record), "utf8");
      await file.sync();
    } finally {
      await file.close();
    }
    // rename 会覆盖已有终态；同目录硬链接原子地只允许一个写入者发布。
    await link(temporary, target);
  } finally {
    await unlink(temporary);
  }
}

export class BackgroundTaskJournal {
  constructor(private readonly root: string) {}

  private path(sessionId: string, taskId: string): string {
    if (!validId(sessionId) || !validId(taskId)) throw new Error("Invalid background task identity");
    return join(this.root, sessionId, `${taskId}.json`);
  }

  async write(record: BackgroundTaskJournalRecord): Promise<void> {
    const launch = this.path(record.sessionId, record.taskId);
    const terminal = this.terminalPath(record.sessionId, record.taskId);
    if (!validToolCallId(record.toolCallId)) throw new Error("Invalid background task tool call identity");
    const current = await this.read(record.sessionId, record.taskId);
    if (current && (current.ownerPid !== record.ownerPid
      || current.toolCallId !== record.toolCallId
      || current.startedAt !== record.startedAt)) {
      throw new Error("Background task owner changed");
    }
    if (record.status === "running") {
      if (current?.status === "running") return;
      if (current) throw new Error("Terminal background task cannot return to running");
      try {
        await publishImmutable(launch, record);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const winner = await this.read(record.sessionId, record.taskId);
        if (winner?.status === "running" && winner.ownerPid === record.ownerPid
          && winner.toolCallId === record.toolCallId && winner.startedAt === record.startedAt) return;
        throw new Error("Background task launch identity changed");
      }
      return;
    }
    if (!current) throw new Error("Background task launch record missing");
    if (current.status !== "running") {
      if (current.status === record.status && current.exitCode === record.exitCode) return;
      throw new Error("Terminal background task cannot change outcome");
    }
    try {
      await publishImmutable(terminal, record);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const winner = await this.read(record.sessionId, record.taskId);
      if (winner?.status === record.status && winner.exitCode === record.exitCode) return;
      throw new Error("Terminal background task cannot change outcome");
    }
  }

  async read(sessionId: string, taskId: string): Promise<BackgroundTaskJournalRecord | null> {
    const launch = await this.readFile(this.path(sessionId, taskId), sessionId, taskId);
    const terminal = await this.readFile(this.terminalPath(sessionId, taskId), sessionId, taskId);
    if (!terminal) return launch;
    if (!launch || launch.status !== "running"
      || terminal.ownerPid !== launch.ownerPid
      || terminal.toolCallId !== launch.toolCallId
      || terminal.startedAt !== launch.startedAt
      || terminal.status === "running") {
      throw new Error("Background task terminal identity mismatch");
    }
    return terminal;
  }

  private terminalPath(sessionId: string, taskId: string): string {
    return `${this.path(sessionId, taskId)}.terminal`;
  }

  private async readFile(target: string, sessionId: string, taskId: string): Promise<BackgroundTaskJournalRecord | null> {
    let raw: string;
    try {
      raw = await readFile(target, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== "object") throw new Error("Invalid background task record");
    const record = value as BackgroundTaskJournalRecord;
    if (record.version !== 1 || record.sessionId !== sessionId || record.taskId !== taskId
      || !validToolCallId(record.toolCallId) || !Number.isInteger(record.ownerPid)
      || !Number.isFinite(record.startedAt) || !Number.isFinite(record.updatedAt)
      || (record.status !== "running" && !Number.isFinite(record.completedAt))
      || (record.exitCode !== undefined && !Number.isInteger(record.exitCode))
      || !["running", "completed", "failed", "timed_out", "cancelled", "spawn_error"].includes(record.status)) {
      throw new Error("Background task identity mismatch");
    }
    return record;
  }
}
