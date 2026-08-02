// Task boundaries for the experiment.
//
// Alice app -- connection --> alice.db <-- connection -- Alice sync
//                                                        ||
//                                      two bounded ChangeBatch channels
//                                                        ||
// Bob app   -- connection --> bob.db   <-- connection -- Bob sync
//
// Each task opens and owns its connection. The driver controls app
// tasks through command handles; the synchronizers exchange owned
// change batches. A connection never leaves the task that opened it.

use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use crate::db::{ChangeBatch, CrrConnection, Replica, ReplicaState, Todo};

// Maximum time allowed for one task request, channel handoff, or
// shutdown. The driver has a separate deadline for overall
// convergence.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

// How often each sync task reads and sends its complete local change
// set.
pub(crate) const SYNC_INTERVAL: Duration = Duration::from_millis(100);

// The experiment driver sends `AppCommand` values to the Alice app
// task and the Bob app task. Each task receives them through its own
// bounded `mpsc` channel and executes the requested database
// operation. Every variant except `Shutdown` contains a
// `oneshot::Sender` used to return the operation's `Result`.
enum AppCommand {
    // Enable WAL, create `todos`, and convert it to a CRR.
    Initialize(oneshot::Sender<Result<()>>),

    // Insert the identical `todo-1` baseline before synchronization
    // begins.
    WriteBaseline(oneshot::Sender<Result<()>>),

    // Perform Alice's or Bob's update and independent row insertion.
    WriteDivergent(oneshot::Sender<Result<()>>),

    // Return the rows in `todos`, ordered by primary key.
    Snapshot(oneshot::Sender<Result<Vec<Todo>>>),

    // Return a consistent snapshot of the rows, DB version, and CRR
    // changes.
    State(oneshot::Sender<Result<ReplicaState>>),

    // Leave the command loop so the task can finalize cr-sqlite and
    // exit.
    Shutdown,
}

// The experiment driver holds one `AppHandle` for Alice's task and
// one for Bob's task. The handle contains no `Connection`; that
// remains inside the corresponding task.
pub(crate) struct AppHandle {
    // Records whether this handle controls Alice's task or Bob's.
    replica: Replica,

    // Sends `AppCommand` values to the task's bounded command
    // channel.
    commands_tx: mpsc::Sender<AppCommand>,

    // Lets the driver await the spawned task and receive its final
    // `Result`.
    join: JoinHandle<Result<()>>,
}

impl AppHandle {
    // Spawn a task that owns a connection to `path`, then wait until
    // the task reports whether opening the database and loading
    // cr-sqlite succeeded.
    pub(crate) async fn spawn(replica: Replica, path: PathBuf) -> Result<Self> {
        // Keep the sending end in `AppHandle`; move the receiving end
        // into the task.
        let (commands_tx, commands_rx) = mpsc::channel(8);

        // Use a separate one-shot channel for the task's startup
        // result.
        let (ready_tx, ready_rx) = oneshot::channel();

        // `join` observes the task's entire lifetime, not just its
        // startup.
        let join = tokio::spawn(app_task(replica, path, commands_rx, ready_tx));

        // Bound the startup wait and distinguish a timeout from a
        // task that exits without reporting whether startup
        // succeeded.
        let ready = match tokio::time::timeout(OPERATION_TIMEOUT, ready_rx).await {
            // The deadline did not expire, and the task sent its
            // startup result.
            Ok(Ok(ready)) => ready,
            // The deadline did not expire, but the task dropped
            // `ready_tx` without sending. Join it to determine how it
            // ended.
            Ok(Err(_)) => match join.await {
                // The task returned successfully without reporting
                // startup, which violates the startup protocol.
                Ok(Ok(())) => {
                    anyhow::bail!("{} app stopped without reporting startup", replica.name())
                }
                // The task returned an error before reporting
                // startup.
                Ok(Err(error)) => {
                    return Err(error)
                        .with_context(|| format!("{} app failed during startup", replica.name()));
                }
                // Joining failed. Because this path has not cancelled
                // the task, the expected cause is a panic.
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("{} app panicked during startup", replica.name())
                    });
                }
            },

            // No startup result arrived before the deadline. Abort
            // and reap the task so that it cannot continue running
            // detached.
            Err(_) => {
                join.abort();
                let _ = join.await;
                anyhow::bail!("{} app startup timed out", replica.name());
            }
        };

        // `ready` is the startup result explicitly sent by the task.
        // If startup failed, reap the task before returning its error
        // message.
        if let Err(message) = ready {
            let _ = join.await;
            anyhow::bail!("{} app failed to start: {message}", replica.name());
        }

        Ok(Self {
            replica,
            commands_tx,
            join,
        })
    }

    // Send `AppCommand::Initialize` to the task controlled by this
    // handle and wait for its result.
    pub(crate) async fn initialize(&self) -> Result<()> {
        self.request("initialize", AppCommand::Initialize).await
    }

    // Insert the shared `todo-1` row before Alice and Bob make their
    // independent changes.
    pub(crate) async fn write_baseline(&self) -> Result<()> {
        self.request("write baseline", AppCommand::WriteBaseline)
            .await
    }

    // Apply this replica's update and independent insertion, creating
    // the divergent state that synchronization must merge.
    pub(crate) async fn write_divergent(&self) -> Result<()> {
        self.request("write divergent changes", AppCommand::WriteDivergent)
            .await
    }

    // Read the current `todos` rows in primary-key order so the
    // driver can compare the two replicas deterministically.
    pub(crate) async fn snapshot(&self) -> Result<Vec<Todo>> {
        self.request("read snapshot", AppCommand::Snapshot).await
    }

    // Capture the replica's rows, database version, and CRR change
    // set for the idempotence check.
    pub(crate) async fn state(&self) -> Result<ReplicaState> {
        self.request("read CRR state", AppCommand::State).await
    }

    // Create a one-shot reply channel, use `command` to embed its
    // sender, and complete the request/reply exchange within
    // `OPERATION_TIMEOUT`.
    async fn request<T>(
        &self,
        action: &'static str,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> AppCommand,
    ) -> Result<T> {
        // Move `reply_tx` into the command while retaining `reply_rx`
        // here to await the task's result.
        let (reply_tx, reply_rx) = oneshot::channel();

        // Identify both the replica and operation in any timeout or
        // channel error returned below.
        let description = format!("{} app: {action}", self.replica.name());

        // Apply one deadline to the complete send-and-reply exchange.
        within_op_timeout(&description, async {
            // Embed `reply_tx` in the command and enqueue it on the
            // task's command channel.
            self.commands_tx
                .send(command(reply_tx))
                .await
                .map_err(|_| anyhow!("command channel closed"))?;
            // Await the one-shot reply. The `?` handles failure to
            // receive; the task's `Result<T>` becomes the result of
            // this async block.
            reply_rx
                .await
                .map_err(|_| anyhow!("response channel closed"))?
        })
        .await
    }

    // Consume the handle, ask its task to leave the command loop, and
    // wait for the task to finalize cr-sqlite and report its result.
    pub(crate) async fn stop(self) -> Result<()> {
        // Make a best-effort shutdown request. If the command cannot
        // be sent, dropping the sender still closes the channel, and
        // the join below reports how the task ended.
        let _ = tokio::time::timeout(
            OPERATION_TIMEOUT,
            self.commands_tx.send(AppCommand::Shutdown),
        )
        .await;
        let result = tokio::time::timeout(OPERATION_TIMEOUT, self.join)
            .await
            .with_context(|| format!("{} app shutdown timed out", self.replica.name()))?
            .with_context(|| format!("{} app task panicked", self.replica.name()))?;
        result.with_context(|| format!("{} app task failed", self.replica.name()))
    }
}

// Open and own one replica's `CrrConnection`, report startup through
// `ready`, then process `AppCommand` values sequentially. Finalize
// cr-sqlite before returning.
async fn app_task(
    replica: Replica,
    path: PathBuf,
    mut commands: mpsc::Receiver<AppCommand>,
    ready: oneshot::Sender<std::result::Result<(), String>>,
) -> Result<()> {
    // Open the database and load cr-sqlite before reporting that the
    // task is ready to accept commands.
    let connection = match CrrConnection::open(&path).await {
        Ok(connection) => connection,
        Err(error) => {
            // Report the startup failure to `AppHandle::spawn`, then
            // return the original error as this task's result.
            let _ = ready.send(Err(format!("{error:#}")));
            return Err(error);
        }
    };
    // Report successful startup. If `AppHandle::spawn` is no longer
    // waiting, no handle can control this task, so finalize and exit.
    if ready.send(Ok(())).is_err() {
        return connection.finalize().await;
    }

    // Execute commands serially on the owned connection. Stop when
    // `Shutdown` arrives or every command sender has been dropped.
    while let Some(command) = commands.recv().await {
        // Each request arm sends the database operation's `Result`
        // through its one-shot reply channel. A failed send means the
        // caller stopped waiting, so there is nothing further to do
        // with that result.
        match command {
            AppCommand::Initialize(reply) => {
                let _ = reply.send(connection.initialize().await);
            }
            AppCommand::WriteBaseline(reply) => {
                let _ = reply.send(connection.write_baseline().await);
            }
            AppCommand::WriteDivergent(reply) => {
                let _ = reply.send(connection.write_divergent(replica).await);
            }
            AppCommand::Snapshot(reply) => {
                let _ = reply.send(connection.snapshot().await);
            }
            AppCommand::State(reply) => {
                let _ = reply.send(connection.state().await);
            }
            AppCommand::Shutdown => break,
        }
    }

    // Let cr-sqlite release its connection-local state before the
    // owned connection is dropped.
    connection.finalize().await
}

// The driver holds one `SyncHandle` for each replica. The handle
// identifies the task and lets the driver observe its completion; the
// database connection remains owned by the task.
pub(crate) struct SyncHandle {
    // Identifies whether this handle tracks Alice's or Bob's
    // synchronizer.
    replica: Replica,

    // Lets the driver inspect whether the synchronizer has stopped
    // and await its final `Result`.
    join: JoinHandle<Result<()>>,
}

impl SyncHandle {
    // Spawn one replica's synchronizer with its database path, peer
    // channels, and shutdown receiver, then wait for it to report
    // whether opening the connection succeeded.
    pub(crate) async fn spawn(
        replica: Replica,
        path: PathBuf,
        to_peer: mpsc::Sender<ChangeBatch>,
        from_peer: mpsc::Receiver<ChangeBatch>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Self> {
        // Use a one-shot channel to report the synchronizer's startup
        // result separately from its eventual task result.
        let (ready_tx, ready_rx) = oneshot::channel();
        // Move the database path, peer channels, shutdown receiver,
        // and startup sender into the task while retaining its
        // `JoinHandle`.
        let join = tokio::spawn(sync_task(
            replica, path, to_peer, from_peer, shutdown, ready_tx,
        ));

        // Apply the same bounded startup handshake as
        // `AppHandle::spawn`: accept an explicit result, diagnose an
        // early exit, or abort on timeout.
        let ready = match tokio::time::timeout(OPERATION_TIMEOUT, ready_rx).await {
            Ok(Ok(ready)) => ready,
            Ok(Err(_)) => match join.await {
                Ok(Ok(())) => {
                    anyhow::bail!("{} sync stopped without reporting startup", replica.name())
                }
                Ok(Err(error)) => {
                    return Err(error)
                        .with_context(|| format!("{} sync failed during startup", replica.name()));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("{} sync panicked during startup", replica.name())
                    });
                }
            },
            Err(_) => {
                join.abort();
                let _ = join.await;
                anyhow::bail!("{} sync startup timed out", replica.name());
            }
        };
        // `ready` is the startup result explicitly sent by
        // `sync_task`. On failure, reap the task before returning its
        // message.
        if let Err(message) = ready {
            let _ = join.await;
            anyhow::bail!("{} sync failed to start: {message}", replica.name());
        }

        // Expose the handle only after the synchronizer has confirmed
        // that its connection is ready.
        Ok(Self { replica, join })
    }

    // Let the driver detect a synchronizer that exits before the
    // planned shutdown instead of waiting for convergence to time
    // out.
    pub(crate) fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    pub(crate) async fn join(self) -> Result<()> {
        let result = tokio::time::timeout(OPERATION_TIMEOUT, self.join)
            .await
            .with_context(|| format!("{} sync shutdown timed out", self.replica.name()))?
            .with_context(|| format!("{} sync task panicked", self.replica.name()))?;
        result.with_context(|| format!("{} sync task failed", self.replica.name()))
    }
}

// Open and own one replica's synchronization connection, run the
// synchronization loop, and finalize cr-sqlite before returning.
async fn sync_task(
    replica: Replica,
    path: PathBuf,
    to_peer: mpsc::Sender<ChangeBatch>,
    from_peer: mpsc::Receiver<ChangeBatch>,
    shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<std::result::Result<(), String>>,
) -> Result<()> {
    let connection = match CrrConnection::open(&path).await {
        Ok(connection) => connection,
        Err(error) => {
            let _ = ready.send(Err(format!("{error:#}")));
            return Err(error);
        }
    };
    if ready.send(Ok(())).is_err() {
        return connection.finalize().await;
    }

    // Finalize cr-sqlite even if the synchronization loop fails.
    let work = sync_loop(replica, &connection, to_peer, from_peer, shutdown).await;
    finish(work, connection.finalize().await)
}

// Periodically send local changes, apply changes received from the
// peer, and stop when shutdown is requested.
async fn sync_loop(
    replica: Replica,
    connection: &CrrConnection,
    to_peer: mpsc::Sender<ChangeBatch>,
    mut from_peer: mpsc::Receiver<ChangeBatch>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // Schedule sends at `SYNC_INTERVAL` without bursting to catch up
    // after a delayed iteration.
    let mut ticker = interval(SYNC_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Wait for shutdown, an incoming batch, or the next scheduled
    // send.
    loop {
        tokio::select! {
            // Stop when the driver requests shutdown or drops the
            // shutdown sender.
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            // Apply each incoming batch; stop if the peer channel
            // closes.
            batch = from_peer.recv() => {
                match batch {
                    Some(batch) => {
                        connection.apply(batch).await.with_context(|| format!("{} applies peer changes", replica.name()))?
                    },
                    None => break,
                }
            }
            // On each tick, read the complete local change set and
            // send it if it is nonempty.
            _ = ticker.tick() => {
                let batch = connection.changes().await.with_context(|| format!("{} reads local changes", replica.name()))?;
                if !batch.is_empty() {
                    match tokio::time::timeout(OPERATION_TIMEOUT, to_peer.send(batch)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => break,
                        Err(_) => anyhow::bail!("{} sync send timed out", replica.name()),
                    }
                }
            }
        }
    }

    Ok(())
}

// Apply the shared operation deadline and add `description` to any
// timeout or operation error.
async fn within_op_timeout<T>(
    description: &str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(OPERATION_TIMEOUT, future)
        .await
        .with_context(|| format!("{description} timed out"))?
        .with_context(|| description.to_owned())
}

// Preserve the work error while also reporting a cleanup failure.
fn finish(work: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}
