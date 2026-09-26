# Durable background Bash readback

## Problem and acceptance

`session/read.projection.backgroundJobs` and the execution adapter's task registry
are process local. During graceful shutdown the execution adapter observes a
terminal cancellation, while runtime shutdown suppresses the synthetic task
notification. After `SIGKILL`, neither the adapter nor the runtime can observe
the Bash process exit. A restarted app-server must never turn a launch record,
empty output file, or diagnostic log into a claimed execution result.

The product must provide a stable native readback addressed by both `sessionId`
and `taskId`. It reports `completed`, `failed`, `timed_out`, `cancelled`, or
`spawn_error` only after the execution owner observes the result. A missing
terminal record yields `unknown`, not a guessed Bash failure or cancellation.

## Ownership and ordering

```text
client -> session runtime -> background Bash execution owner
                               | launch/terminal state
                               v
                     durable session/task journal
                               |
                       session/task readback
                               v
                   desktop and app-server clients
```

The execution owner is the sole producer of terminal status. The durable journal
is the source for recovery through `session/backgroundTaskRead`; existing live
projection/events/notifications remain supplemental delivery paths. One journal
identity is keyed by `(sessionId, taskId)` and includes `toolCallId`, status,
timestamps, owner identity and optional exit code. User commands and tool output
are not stored in this record. A read for a wrong session/task pair returns no
result. An immutable launch file and a separate immutable terminal file are
published with an atomic create-if-absent operation on the same filesystem.
Readers prefer the terminal file. Competing terminal outcomes cannot both win;
identical retries are idempotent, and a terminal state cannot regress to
`running`. A terminal read must also verify that its owner, tool call, start
time, session ID and task ID match the immutable launch record. Existing
single-file records remain readable during migration.
The native tool-call ID is data, not a filesystem path component. Model providers
may emit IDs such as `Bash:0`; validation accepts that form while session and
task IDs remain restricted to safe path components. Windows background launch
must be exercised with such a provider-generated ID.

1. Persist `running` before acknowledging a task ID.
2. Observe the actual process exit and persist the terminal state before
   publishing an event or notification.
   A persisted output limit stop is `failed` with an `output_limit` error in
   both the supervisor journal and the execution result. It is not user
   cancellation; a timeout is `timed_out` in both places.
3. On graceful shutdown, stop and wait for owned Bash tasks, persist their
   observed outcomes, then release session storage. No model turn is started
   by this persistence step.
4. On app-server hard death, a separate execution supervisor must retain
   ownership of Bash and persist its actual exit. The app-server cannot do this
   after `SIGKILL`. A supervisor failure leaves the native result `unknown`;
   it cannot invent an exit code.
   The supervisor receives launch configuration and explicit stop requests
   over Node's cross-platform IPC channel. IPC disconnection after
   app-server death is not cancellation. Once Bash exits, it persists the
   result and disconnects IPC so a natural Bash exit leaves no supervisor.
   Do not use `fs.ReadStream` for an inherited control pipe: on macOS its
   blocked libuv worker can prevent even `process.exit` from completing.
   The launch configuration carries the resolved child environment through
   private IPC. Supervised Bash must receive the same sanitized
   environment overlay as ordinary Bash execution.
   The supervisor keeps the launched Bash as its child. It must not spawn a
   second copy, and cancellation targets the Bash process tree while the
   supervisor stays alive long enough to observe and persist the exit.
   A `running` journal row is only a launch fact. Even in the original
   app-server process, native readback may return `running` only while the
   execution adapter still has the same active task. If its supervisor dies,
   readback becomes `unknown`, never a fabricated Bash failure or live task.
5. On resume, query the journal by both IDs for cards whose session snapshot is
   `unknown` or `running`; verified terminal readback wins over stale live
   projections. Existing desktop continuous and remote replayable delivery
   remain supplemental to this native lookup.

## Failure and migration boundaries

- Journal write failure before launch acknowledgement fails the launch closed.
- Journal write failure after a real terminal observation remains an explicit
  readback failure; clients cannot display a guessed terminal state.
- Existing sessions without journal records keep their current safe unknown
  state. No migration may infer a result from historical launch messages.
- Official desktop task deletion is a list tombstone and retains native session
  history. The matching task records remain readable if that native session is
  explicitly resumed; OpenCodian hides the deleted session from its browser.
- A multi-process owner must use fencing so an old process cannot overwrite a
  newer task generation or a record belonging to another session.

## Required tests

- Natural success and nonzero exit; explicit cancel; graceful shutdown;
  `SIGKILL` of the app-server while Bash runs and while it exits; supervisor
  death; concurrent tasks in one session and tasks in adjacent sessions.
- Reconnect and read by both IDs; wrong-session query; repeated read; old event
  after new read; duplicate terminal write; storage error at launch and exit.
- Real visible desktop task card and a separate app-server client, each paired
  with native request and readback. No test may accept log text or stdout file
  existence as terminal proof.
- Windows execution uses Node IPC and the configured native shell provider.
  Verify success, failure, explicit cancellation, and parent-process loss on a
  Windows Test Vault before claiming cross-platform runtime acceptance.
  The supervisor must be spawned detached on Windows; otherwise the desktop
  host's process lifetime can end the supervisor alongside the app-server,
  leaving a running launch row with no execution owner.
