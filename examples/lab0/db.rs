// Database types and operations for one replica.
//
// `CrrConnection` wraps a libSQL connection with cr-sqlite loaded. It
// provides local writes, CRR change exchange, state snapshots, and
// extension finalization.
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use libsql::{
    Builder, Connection, LoadExtensionGuard, TransactionBehavior, Value, params_from_iter,
};

// Path to the Cargo-built `crsqlite` dynamic library, supplied by the
// artifact dependency at compile time.
pub(crate) const CRSQLITE_LIB: &str = env!("CARGO_CDYLIB_FILE_CRSQLITE");

// Identifies Alice or Bob for replica-specific writes and diagnostic
// messages.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Replica {
    Alice,
    Bob,
}

impl Replica {
    // Return the lowercase label used in diagnostic messages.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Alice => "alice",
            Self::Bob => "bob",
        }
    }
}

// In-memory representation of a `todos` row used to compare replica
// snapshots.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Todo {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) completed: bool,
}

// An owned Rust representation of one row from cr-sqlite's
// extension-defined `crsql_changes` virtual table.
//
// The field list and ordering come from `changesConnect` in
// `core/src/changes-vtab.c` at the revision pinned in `Cargo.toml`.
//
// This is generic replication protocol data, not part of the `todos`
// model. The lab mirrors all nine columns so a synchronizer can read
// a change from one replica and insert it unchanged into the other.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrsqlChange {
    // Name of the CRR table containing the changed row.
    table: String,
    // Packed representation of the row's primary-key values.
    pk: Vec<u8>,
    // Changed column name, or a cr-sqlite row-lifecycle sentinel.
    cid: String,
    // SQLite value associated with the change.
    val: Value,
    // Logical version of the changed column.
    col_version: i64,
    // Database-wide version assigned to the transaction containing
    // the change.
    db_version: i64,
    // Identifier of the replica where the change originated.
    site_id: Vec<u8>,
    // Causal length used to order deletion and recreation of a row.
    cl: i64,
    // Sequence number ordering changes within a database version.
    seq: i64,
}

impl CrsqlChange {
    // Convert the change back into SQLite values in the exact column
    // order required by `crsql_changes`.
    fn values(&self) -> Vec<Value> {
        vec![
            Value::Text(self.table.clone()),
            Value::Blob(self.pk.clone()),
            Value::Text(self.cid.clone()),
            self.val.clone(),
            Value::Integer(self.col_version),
            Value::Integer(self.db_version),
            Value::Blob(self.site_id.clone()),
            Value::Integer(self.cl),
            Value::Integer(self.seq),
        ]
    }
}

// Change rows sent in one channel message and applied to the peer in
// one transaction.
pub(crate) type ChangeBatch = Vec<CrsqlChange>;

// A consistent snapshot of one replica's application data and CRR
// metadata, used to verify that further replay is idempotent.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReplicaState {
    // Materialized application rows at the time of the snapshot.
    todos: Vec<Todo>,
    // Current database-wide CRR version.
    db_version: i64,
    // Complete change set currently exposed by `crsql_changes`.
    changes: ChangeBatch,
}

// A libSQL connection with cr-sqlite loaded and verified.
pub(crate) struct CrrConnection {
    connection: Connection,
}

impl CrrConnection {
    // Open a local libSQL database, load cr-sqlite into its
    // connection, verify the extension, and configure contention
    // handling.
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        // Build libSQL's database handle for the on-disk file.
        let database = Builder::new_local(path)
            .build()
            .await
            .with_context(|| format!("open {}", path.display()))?;
        // Open the independent connection that this task will own.
        let connection = database
            .connect()
            .with_context(|| format!("connect to {}", path.display()))?;

        // Temporarily enable extension loading; dropping the guard
        // disables it again.
        let guard = LoadExtensionGuard::new(&connection).context("enable extension loading")?;
        // Load the Cargo-built dynamic library through its SQLite
        // extension entry point.
        connection
            .load_extension(CRSQLITE_LIB, Some("sqlite3_crsqlite_init"))
            .with_context(|| format!("load {CRSQLITE_LIB}"))?;
        drop(guard);

        // Call an extension-provided function to verify that
        // cr-sqlite loaded successfully and that this database has a
        // site identity.
        let site_id = single_value(&connection, "SELECT crsql_site_id()")
            .await
            .context("verify cr-sqlite")?;
        ensure!(
            matches!(site_id, Value::Blob(ref bytes) if !bytes.is_empty()),
            "crsql_site_id() did not return a nonempty blob"
        );

        // Bound how long this connection waits when the other task's
        // connection holds a SQLite lock.
        connection
            .busy_timeout(Duration::from_secs(2))
            .context("set busy timeout")?;

        Ok(Self { connection })
    }

    // Enable WAL, create the application table, and register it with
    // cr-sqlite as a CRR.
    pub(crate) async fn initialize(&self) -> Result<()> {
        // Use WAL so the application and synchronizer connections can
        // read without blocking a writer.
        let journal_mode = single_value(&self.connection, "PRAGMA journal_mode=WAL")
            .await
            .context("enable WAL")?;
        ensure!(
            matches!(journal_mode, Value::Text(ref mode) if mode.eq_ignore_ascii_case("wal")),
            "PRAGMA journal_mode=WAL returned {journal_mode:?}"
        );

        // Create an ordinary SQLite table before asking cr-sqlite to
        // add CRR behavior.
        self.connection
            .execute(
                "CREATE TABLE todos (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL DEFAULT '', completed INTEGER NOT NULL DEFAULT 0)",
                (),
            )
            .await
            .context("create todos")?;
        // Ask cr-sqlite to install its metadata tables and triggers
        // for `todos`.
        single_value(&self.connection, "SELECT crsql_as_crr('todos')")
            .await
            .context("enable CRR for todos")?;
        Ok(())
    }

    // Insert the same shared row on both replicas before they make
    // independent updates.
    pub(crate) async fn write_baseline(&self) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO todos (id, title, completed) VALUES (?1, ?2, ?3)",
                ("todo-1", "buy milk", 0_i64),
            )
            .await
            .context("insert baseline todo")?;
        Ok(())
    }

    // Apply the replica-specific update and insertion that create the
    // divergent state for the experiment.
    pub(crate) async fn write_divergent(&self, replica: Replica) -> Result<()> {
        match replica {
            Replica::Alice => {
                self.connection
                    .execute(
                        "UPDATE todos SET title = ?1 WHERE id = ?2",
                        ("buy oat milk", "todo-1"),
                    )
                    .await
                    .context("Alice updates the shared title")?;
                self.connection
                    .execute(
                        "INSERT INTO todos (id, title, completed) VALUES (?1, ?2, ?3)",
                        ("todo-alice", "call the plumber", 0_i64),
                    )
                    .await
                    .context("Alice inserts her todo")?;
            }
            Replica::Bob => {
                self.connection
                    .execute(
                        "UPDATE todos SET completed = ?1 WHERE id = ?2",
                        (1_i64, "todo-1"),
                    )
                    .await
                    .context("Bob completes the shared todo")?;
                self.connection
                    .execute(
                        "INSERT INTO todos (id, title, completed) VALUES (?1, ?2, ?3)",
                        ("todo-bob", "book the dentist", 0_i64),
                    )
                    .await
                    .context("Bob inserts his todo")?;
            }
        }
        Ok(())
    }

    // Read the replica's materialized `todos` rows in deterministic
    // order.
    pub(crate) async fn snapshot(&self) -> Result<Vec<Todo>> {
        read_todos(&self.connection).await
    }

    // Read the application rows, database version, and change set
    // within one transaction so they form a consistent snapshot.
    pub(crate) async fn state(&self) -> Result<ReplicaState> {
        let transaction = self.connection.transaction().await?;
        let todos = read_todos(&transaction).await?;
        let db_version = read_db_version(&transaction).await?;
        let changes = read_changes(&transaction).await?;
        transaction.commit().await?;
        Ok(ReplicaState {
            todos,
            db_version,
            changes,
        })
    }

    // Read the complete change set for the next outbound
    // synchronization batch.
    pub(crate) async fn changes(&self) -> Result<ChangeBatch> {
        read_changes(&self.connection).await
    }

    // Merge a peer's change batch by inserting it into
    // `crsql_changes` within one transaction.
    pub(crate) async fn apply(&self, changes: ChangeBatch) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        // Acquire SQLite's write lock before applying any part of the
        // batch.
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin change batch")?;
        // Prepare one positional insert matching the schema mirrored
        // by `CrsqlChange`, and reuse it for every row in the batch.
        let statement = transaction
            .prepare("INSERT INTO crsql_changes VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")
            .await
            .context("prepare change insertion")?;

        // Each insertion invokes cr-sqlite's merge logic; it does not
        // append a row to an ordinary SQLite table.
        for change in changes {
            statement
                .execute(params_from_iter(change.values()))
                .await
                .context("apply CRR change")?;
            statement.reset();
        }

        // Release the prepared statement before committing the
        // transaction.
        drop(statement);
        transaction.commit().await.context("commit change batch")?;
        Ok(())
    }

    // Run cr-sqlite's connection cleanup before the libSQL connection
    // is dropped.
    pub(crate) async fn finalize(&self) -> Result<()> {
        single_value(&self.connection, "SELECT crsql_finalize()")
            .await
            .context("finalize cr-sqlite")?;
        Ok(())
    }
}

// Execute a query that must return exactly one row and return its
// first column.
async fn single_value(connection: &Connection, sql: &str) -> Result<Value> {
    let mut rows = connection.query(sql, ()).await?;
    let row = rows
        .next()
        .await?
        .with_context(|| format!("{sql} returned no row"))?;
    let value = row.get_value(0)?;
    ensure!(rows.next().await?.is_none(), "{sql} returned multiple rows");
    Ok(value)
}

// Materialize every `todos` row in primary-key order.
async fn read_todos(connection: &Connection) -> Result<Vec<Todo>> {
    let mut rows = connection
        .query("SELECT id, title, completed FROM todos ORDER BY id", ())
        .await?;
    let mut todos = Vec::new();
    while let Some(row) = rows.next().await? {
        todos.push(Todo {
            id: row.get(0)?,
            title: row.get(1)?,
            completed: row.get::<i64>(2)? != 0,
        });
    }
    Ok(todos)
}

// Read and type-check the database-wide version maintained by
// cr-sqlite.
async fn read_db_version(connection: &Connection) -> Result<i64> {
    match single_value(connection, "SELECT crsql_db_version()").await? {
        Value::Integer(version) => Ok(version),
        value => anyhow::bail!("crsql_db_version() returned {value:?}"),
    }
}

// Materialize the complete `crsql_changes` virtual table as owned
// changes in deterministic order.
async fn read_changes(connection: &Connection) -> Result<ChangeBatch> {
    // Select all nine protocol columns; the trailing sort keys make
    // the order deterministic when version and sequence values tie.
    let mut rows = connection
        .query(
            "SELECT \"table\", pk, cid, val, col_version, db_version, site_id, cl, seq FROM crsql_changes ORDER BY db_version, seq, \"table\", pk, cid, site_id",
            (),
        )
        .await?;
    let mut changes = Vec::new();
    while let Some(row) = rows.next().await? {
        changes.push(CrsqlChange {
            table: row.get(0)?,
            pk: row.get(1)?,
            cid: row.get(2)?,
            val: row.get_value(3)?,
            col_version: row.get(4)?,
            db_version: row.get(5)?,
            site_id: row.get(6)?,
            cl: row.get(7)?,
            seq: row.get(8)?,
        });
    }
    Ok(changes)
}
