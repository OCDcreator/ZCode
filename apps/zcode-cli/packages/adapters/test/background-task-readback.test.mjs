import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BackgroundTaskJournal } from "../src/exec/background-task-journal.ts";
import { NodeExecutionAdapter } from "../src/exec/node-execution-adapter.ts";

const adapterModule = fileURLToPath(new URL("../src/exec/node-execution-adapter.ts", import.meta.url));

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-readback-"));
  const sessionId = "sess_test";
  const adapter = new NodeExecutionAdapter({
    outputRootDir: join(root, "exec"),
    backgroundTaskJournalRoot: join(root, "journal"),
  });
  const journal = new BackgroundTaskJournal(join(root, "journal"));
  const run = async (command, callId = "call_test") => {
    const result = await adapter.runBashWithBackgroundLifecycle({
      command: { mode: "shell", command, shellProfile: "posix-bash" },
      cwd: root,
      trace: { sessionId, traceId: "trace_test", attributes: { toolCallId: callId } },
    }, { mode: "explicit" });
    assert.equal(result.kind, "backgrounded");
    return result.task.taskId;
  };
  return { adapter, journal, root, run, sessionId };
}

test("observed success and failure are durable and scoped to both IDs", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  try {
    const successId = await f.run("sleep 1; exit 0", "call_success");
    assert.equal((await f.journal.read(f.sessionId, successId))?.status, "running");
    await f.adapter.waitForBackgroundTask(successId);
    assert.match((await f.journal.read(f.sessionId, successId))?.status ?? "", /completed/);
    assert.equal((await f.journal.read(f.sessionId, successId))?.exitCode, 0);
    assert.equal(await f.journal.read("sess_adjacent", successId), null);

    const failedId = await f.run("sleep 1; exit 7", "call_failure");
    await f.adapter.waitForBackgroundTask(failedId);
    assert.equal((await f.journal.read(f.sessionId, failedId))?.status, "failed");
    assert.equal((await f.journal.read(f.sessionId, failedId))?.exitCode, 7);
  } finally {
    await f.adapter.close();
  }
});

test("cancellation waits for the supervisor's durable terminal write", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  try {
    const taskId = await f.run("sleep 30");
    assert.equal((await f.adapter.cancelBackgroundTask(taskId))?.status, "cancelled");
    assert.equal((await f.journal.read(f.sessionId, taskId))?.status, "cancelled");
  } finally {
    await f.adapter.close();
  }
});

test("timeout and persisted output limit agree with durable supervisor state", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  try {
    const timed = await f.adapter.start({
      command: { mode: "shell", command: "sleep 30", shellProfile: "posix-bash" },
      cwd: f.root,
      timeoutMs: 500,
      trace: { sessionId: f.sessionId, traceId: "trace_timeout", attributes: { toolCallId: "call_timeout" } },
    });
    const timedResult = await f.adapter.waitForBackgroundTask(timed.taskId);
    assert.equal(timedResult?.status, "timed_out");
    assert.equal((await f.journal.read(f.sessionId, timed.taskId))?.status, timedResult.status);

    const limited = await f.adapter.start({
      command: { mode: "shell", command: "printf '12345678901234567890'; sleep 30", shellProfile: "posix-bash" },
      cwd: f.root,
      timeoutMs: 10_000,
      outputLimit: { persistOutput: "always", maxPersistedBytes: 10, killProcessOnPersistedLimit: true },
      trace: { sessionId: f.sessionId, traceId: "trace_limit", attributes: { toolCallId: "call_limit" } },
    });
    const limitedResult = await f.adapter.waitForBackgroundTask(limited.taskId);
    assert.equal(limitedResult?.status, "failed", JSON.stringify({
      persisted: await f.journal.read(f.sessionId, limited.taskId),
      wrapper: f.adapter.backgroundTasks.get(limited.taskId),
    }));
    assert.equal((await f.journal.read(f.sessionId, limited.taskId))?.status, limitedResult.status);
  } finally {
    await f.adapter.close();
  }
});

test("graceful adapter close persists a real cancelled exit", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  const taskId = await f.run("sleep 30", "call_close");
  await f.adapter.close();
  assert.equal((await f.journal.read(f.sessionId, taskId))?.status, "cancelled");
});

test("a journal launch failure does not acknowledge a background task", async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-blocked-journal-"));
  const blockedRoot = join(root, "journal-file");
  await writeFile(blockedRoot, "blocked");
  const adapter = new NodeExecutionAdapter({
    outputRootDir: join(root, "exec"), backgroundTaskJournalRoot: blockedRoot,
  });
  try {
    await assert.rejects(adapter.runBashWithBackgroundLifecycle({
      command: { mode: "shell", command: "sleep 30", shellProfile: "posix-bash" },
      cwd: root,
      trace: { sessionId: "sess_blocked", traceId: "trace_blocked", attributes: { toolCallId: "call_blocked" } },
    }, { mode: "explicit" }));
  } finally {
    await adapter.close();
  }
});

test("competing terminal outcomes cannot both commit or regress to running", async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-terminal-race-"));
  const journal = new BackgroundTaskJournal(root);
  const base = {
    version: 1, sessionId: "sess_race", taskId: "exec_race", toolCallId: "call_race",
    startedAt: 1, updatedAt: 2, ownerPid: process.pid,
  };
  await journal.write({ ...base, status: "running" });
  const completed = { ...base, status: "completed", completedAt: 3, exitCode: 0 };
  const failed = { ...base, status: "failed", completedAt: 3, exitCode: 7 };
  const results = await Promise.allSettled([journal.write(completed), journal.write(failed)]);
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  const winner = results[0].status === "fulfilled" ? completed : failed;
  assert.equal((await journal.read(base.sessionId, base.taskId))?.status, winner.status);
  await journal.write(winner);
  await assert.rejects(journal.write({ ...base, status: "running" }));
});

test("provider tool-call IDs with a colon are durable data, not path segments", async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-colon-call-"));
  const journal = new BackgroundTaskJournal(root);
  const launch = {
    version: 1, sessionId: "sess_colon", taskId: "exec_colon", toolCallId: "Bash:0",
    status: "running", startedAt: 1, updatedAt: 1, ownerPid: process.pid,
  };
  await journal.write(launch);
  await journal.write({ ...launch, status: "completed", completedAt: 2, updatedAt: 2, exitCode: 0 });
  assert.equal((await journal.read(launch.sessionId, launch.taskId))?.status, "completed");
  await assert.rejects(journal.write({ ...launch, sessionId: "../other" }));
});

test("legacy single-file terminal records remain readable", async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-legacy-journal-"));
  const sessionId = "sess_legacy";
  const record = {
    version: 1, sessionId, taskId: "exec_legacy", toolCallId: "call_legacy",
    status: "completed", startedAt: 1, updatedAt: 2, completedAt: 2,
    exitCode: 0, ownerPid: process.pid,
  };
  await mkdir(join(root, sessionId));
  await writeFile(join(root, sessionId, `${record.taskId}.json`), JSON.stringify(record));
  const journal = new BackgroundTaskJournal(root);
  assert.deepEqual(await journal.read(sessionId, record.taskId), record);
  await journal.write(record);
});

test("a terminal file with a different launch owner is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-mismatched-terminal-"));
  const journal = new BackgroundTaskJournal(root);
  const launch = {
    version: 1, sessionId: "sess_identity", taskId: "exec_identity",
    toolCallId: "call_identity", status: "running", startedAt: 1,
    updatedAt: 1, ownerPid: process.pid,
  };
  await journal.write(launch);
  await writeFile(join(root, launch.sessionId, `${launch.taskId}.json.terminal`), JSON.stringify({
    ...launch, status: "completed", completedAt: 2, updatedAt: 2,
    exitCode: 0, ownerPid: process.pid + 1,
  }));
  await assert.rejects(journal.read(launch.sessionId, launch.taskId), /identity mismatch/i);
});

test("the supervised child receives the resolved execution environment", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  try {
    const result = await f.adapter.runBashWithBackgroundLifecycle({
      command: { mode: "shell", command: "test \"$ZCODE_TEST_OVERLAY\" = durable", shellProfile: "posix-bash" },
      cwd: f.root,
      env: { set: { ZCODE_TEST_OVERLAY: "durable" } },
      trace: { sessionId: f.sessionId, traceId: "trace_env", attributes: { toolCallId: "call_env" } },
    }, { mode: "explicit" });
    assert.equal(result.kind, "backgrounded");
    assert.equal((await f.adapter.waitForBackgroundTask(result.task.taskId))?.status, "completed");
    assert.equal((await f.journal.read(f.sessionId, result.task.taskId))?.status, "completed");
  } finally {
    await f.adapter.close();
  }
});

test("a killed supervisor cannot turn its own exit into a Bash failure", { skip: process.platform === "win32" }, async () => {
  const f = await fixture();
  try {
    const result = await f.adapter.runBashWithBackgroundLifecycle({
      command: { mode: "shell", command: "sleep 2", shellProfile: "posix-bash" },
      cwd: f.root,
      trace: { sessionId: f.sessionId, traceId: "trace_supervisor_loss", attributes: { toolCallId: "call_supervisor_loss" } },
    }, { mode: "explicit" });
    assert.equal(result.kind, "backgrounded");
    assert.ok(result.task.pid);
    process.kill(result.task.pid, "SIGKILL");
    assert.equal(await f.adapter.waitForBackgroundTask(result.task.taskId), undefined);
    assert.equal((await f.journal.read(f.sessionId, result.task.taskId))?.status, "running");
  } finally {
    await f.adapter.close();
  }
});

test("supervisor records actual exit after its app-server owner is killed", { skip: process.platform === "win32" }, async () => {
  const root = await mkdtemp(join(tmpdir(), "zcode-task-owner-loss-"));
  const sessionId = "sess_owner_loss";
  const code = `import { NodeExecutionAdapter } from ${JSON.stringify(adapterModule)};
    const adapter = new NodeExecutionAdapter({outputRootDir:${JSON.stringify(join(root, "exec"))},backgroundTaskJournalRoot:${JSON.stringify(join(root, "journal"))}});
    const result = await adapter.runBashWithBackgroundLifecycle({command:{mode:"shell",command:"sleep 2; exit 0",shellProfile:"posix-bash"},cwd:${JSON.stringify(root)},trace:{sessionId:${JSON.stringify(sessionId)},traceId:"trace_owner_loss",attributes:{toolCallId:"call_owner_loss"}}},{mode:"explicit"});
    console.log(result.task.taskId);setInterval(()=>{},1000);`;
  const owner = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", code], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const taskId = await new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error("owner launch timed out")), 8_000);
    owner.stdout.on("data", (chunk) => {
      output += chunk;
      if (!output.includes("\n")) return;
      clearTimeout(timer);
      resolve(output.split("\n")[0]);
    });
    owner.once("error", reject);
  });
  const journal = new BackgroundTaskJournal(join(root, "journal"));
  assert.equal((await journal.read(sessionId, taskId))?.status, "running");
  owner.kill("SIGKILL");
  await new Promise((resolve) => owner.once("exit", resolve));
  const deadline = Date.now() + 8_000;
  let final;
  while (Date.now() < deadline) {
    final = await journal.read(sessionId, taskId);
    if (final?.status !== "running") break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.equal(final?.status, "completed");
  assert.equal(final.exitCode, 0);
});
