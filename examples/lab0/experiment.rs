use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep};

use crate::db::{Replica, Todo};
use crate::tasks::{AppHandle, SYNC_INTERVAL, SyncHandle};

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const IDEMPOTENCE_WINDOW: Duration = Duration::from_millis(600);

pub(crate) async fn run() -> Result<()> {
    let directory = tempfile::tempdir().context("create experiment directory")?;
    let alice_path = directory.path().join("alice.db");
    let bob_path = directory.path().join("bob.db");

    let alice = AppHandle::spawn(Replica::Alice, alice_path.clone()).await?;
    let bob = match AppHandle::spawn(Replica::Bob, bob_path.clone()).await {
        Ok(bob) => bob,
        Err(error) => return finish(Err(error), alice.stop().await),
    };

    let work = run_with_apps(&alice, &bob, &alice_path, &bob_path).await;
    let (alice_cleanup, bob_cleanup) = tokio::join!(alice.stop(), bob.stop());
    finish(work, combine(alice_cleanup, bob_cleanup))
}

async fn run_with_apps(
    alice: &AppHandle,
    bob: &AppHandle,
    alice_path: &Path,
    bob_path: &Path,
) -> Result<()> {
    tokio::try_join!(alice.initialize(), bob.initialize())?;
    tokio::try_join!(alice.write_baseline(), bob.write_baseline())?;
    tokio::try_join!(alice.write_divergent(), bob.write_divergent())?;
    println!("alice and bob wrote independently");

    let (alice_to_bob_tx, alice_to_bob_rx) = mpsc::channel(2);
    let (bob_to_alice_tx, bob_to_alice_rx) = mpsc::channel(2);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

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

    let work = prove_convergence(alice, bob, &alice_sync, &bob_sync).await;
    let _ = shutdown_tx.send(true);
    let (alice_cleanup, bob_cleanup) = tokio::join!(alice_sync.join(), bob_sync.join());
    finish(work, combine(alice_cleanup, bob_cleanup))
}

async fn prove_convergence(
    alice: &AppHandle,
    bob: &AppHandle,
    alice_sync: &SyncHandle,
    bob_sync: &SyncHandle,
) -> Result<()> {
    let expected = expected_todos();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let (mut alice_snapshot, mut bob_snapshot) =
        tokio::try_join!(alice.snapshot(), bob.snapshot())?;

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

fn ensure_syncs_running(alice: &SyncHandle, bob: &SyncHandle) -> Result<()> {
    ensure!(!alice.is_finished(), "Alice sync task exited early");
    ensure!(!bob.is_finished(), "Bob sync task exited early");
    Ok(())
}

fn combine(first: Result<()>, second: Result<()>) -> Result<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(second)) => {
            Err(error.context(format!("another task also failed: {second:#}")))
        }
    }
}

fn finish(work: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (work, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}
