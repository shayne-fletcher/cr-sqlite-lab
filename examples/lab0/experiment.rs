// Experiment driver and lifecycle.
//
// This module creates the two replicas, directs their independent
// writes, starts synchronization, verifies convergence and
// idempotence, and shuts every task down.
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep};

use crate::db::{Replica, Todo};
use crate::tasks::{AppHandle, SYNC_INTERVAL, SyncHandle};

// Maximum time allowed for both replicas to reach the expected state.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
// Delay between snapshot checks while waiting for convergence.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
// Time during which continued synchronization must leave both
// replicas unchanged.
const IDEMPOTENCE_WINDOW: Duration = Duration::from_millis(600);

// Create both replicas and their application tasks, run the
// experiment, and stop both tasks before returning.
pub(crate) async fn run() -> Result<()> {
    // Keep both database files in a temporary directory whose
    // lifetime covers every task.
    let directory = tempfile::tempdir().context("create experiment directory")?;
    let alice_path = directory.path().join("alice.db");
    let bob_path = directory.path().join("bob.db");

    // Start one application task per replica. If Bob cannot start,
    // stop the already-running Alice task before returning.
    let alice = AppHandle::spawn(Replica::Alice, alice_path.clone()).await?;
    let bob = match AppHandle::spawn(Replica::Bob, bob_path.clone()).await {
        Ok(bob) => bob,
        Err(error) => return finish(Err(error), alice.stop().await),
    };

    // Preserve the experiment result while always stopping both
    // application tasks.
    let work = run_with_apps(&alice, &bob, &alice_path, &bob_path).await;
    // Stop Alice and Bob concurrently.
    let (alice_cleanup, bob_cleanup) = tokio::join!(alice.stop(), bob.stop());
    finish(work, combine(alice_cleanup, bob_cleanup))
}

// Prepare both databases, perform the independent writes, run
// synchronization, and shut the synchronizers down.
async fn run_with_apps(
    alice: &AppHandle,
    bob: &AppHandle,
    alice_path: &Path,
    bob_path: &Path,
) -> Result<()> {
    // Advance both replicas through each phase together before
    // beginning the next one.
    tokio::try_join!(alice.initialize(), bob.initialize())?;
    tokio::try_join!(alice.write_baseline(), bob.write_baseline())?;
    tokio::try_join!(alice.write_divergent(), bob.write_divergent())?;
    println!("alice and bob wrote independently");

    // Create one bounded change channel in each direction between the
    // two synchronizers.
    let (alice_to_bob_tx, alice_to_bob_rx) = mpsc::channel(2);
    let (bob_to_alice_tx, bob_to_alice_rx) = mpsc::channel(2);
    // Use one watch channel to request shutdown from both
    // synchronizers.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Start one synchronizer per replica with crossed channel
    // endpoints. If Bob cannot start, signal and join the
    // already-running Alice task.
    let alice_sync = SyncHandle::spawn(
        Replica::Alice,
        alice_path.to_path_buf(),
        alice_to_bob_tx,
        bob_to_alice_rx,
        shutdown_rx.clone(),
    )
    .await?;
    let bob_sync = match SyncHandle::spawn(
        Replica::Bob,
        bob_path.to_path_buf(),
        bob_to_alice_tx,
        alice_to_bob_rx,
        shutdown_rx,
    )
    .await
    {
        Ok(sync) => sync,
        Err(error) => {
            let _ = shutdown_tx.send(true);
            return finish(Err(error), alice_sync.join().await);
        }
    };

    // Preserve the proof result while always signalling and joining both
    // synchronizers.
    let work = prove_convergence(alice, bob, &alice_sync, &bob_sync).await;
    // Set the shared shutdown flag and wake both synchronizers.
    let _ = shutdown_tx.send(true);
    // Join both synchronizers concurrently.
    let (alice_cleanup, bob_cleanup) = tokio::join!(alice_sync.join(), bob_sync.join());
    finish(work, combine(alice_cleanup, bob_cleanup))
}

// Wait until both replicas contain the expected rows, then verify
// that continued synchronization leaves their complete state
// unchanged.
async fn prove_convergence(
    alice: &AppHandle,
    bob: &AppHandle,
    alice_sync: &SyncHandle,
    bob_sync: &SyncHandle,
) -> Result<()> {
    // Use one absolute deadline for the entire convergence check.
    let expected = expected_todos();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let (mut alice_snapshot, mut bob_snapshot) =
        tokio::try_join!(alice.snapshot(), bob.snapshot())?;

    // Poll both snapshots until they match the expected state,
    // failing early if either synchronizer exits.
    loop {
        ensure_syncs_running(alice_sync, bob_sync)?;
        if alice_snapshot == expected && bob_snapshot == expected {
            break;
        }
        if Instant::now() >= deadline {
            bail!("replicas did not converge; Alice: {alice_snapshot:?}; Bob: {bob_snapshot:?}");
        }
        sleep(POLL_INTERVAL).await;
        (alice_snapshot, bob_snapshot) = tokio::try_join!(alice.snapshot(), bob.snapshot())?;
    }

    println!("background synchronization converged");
    ensure!(alice_snapshot.len() == 3, "expected three todos");
    println!("three todos present on both replicas");

    // Capture both complete states, allow several more
    // synchronization rounds, and require the states to remain
    // unchanged.
    let (alice_before, bob_before) = tokio::try_join!(alice.state(), bob.state())?;
    sleep(IDEMPOTENCE_WINDOW.max(SYNC_INTERVAL)).await;
    ensure_syncs_running(alice_sync, bob_sync)?;
    let (alice_after, bob_after) = tokio::try_join!(alice.state(), bob.state())?;
    ensure!(
        alice_before == alice_after,
        "Alice changed during idempotence window"
    );
    ensure!(
        bob_before == bob_after,
        "Bob changed during idempotence window"
    );
    println!("additional synchronization was idempotent");
    println!("CRR convergence verified");
    Ok(())
}

// Define the expected merge: Alice's title update, Bob's completion,
// and both independently inserted rows.
fn expected_todos() -> Vec<Todo> {
    vec![
        Todo {
            id: "todo-1".into(),
            title: "buy oat milk".into(),
            completed: true,
        },
        Todo {
            id: "todo-alice".into(),
            title: "call the plumber".into(),
            completed: false,
        },
        Todo {
            id: "todo-bob".into(),
            title: "book the dentist".into(),
            completed: false,
        },
    ]
}

// Fail immediately if either synchronizer exits before shutdown.
fn ensure_syncs_running(alice: &SyncHandle, bob: &SyncHandle) -> Result<()> {
    ensure!(!alice.is_finished(), "Alice sync task exited early");
    ensure!(!bob.is_finished(), "Bob sync task exited early");
    Ok(())
}

// Combine two independent results, preserving the first error and
// attaching the second if both fail.
fn combine(first: Result<()>, second: Result<()>) -> Result<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(second)) => {
            Err(error.context(format!("another task also failed: {second:#}")))
        }
    }
}

// Return the work error if present while retaining any cleanup
// failure as additional context.
fn finish(work: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}
