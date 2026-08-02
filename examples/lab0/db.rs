use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use libsql::{
    Builder, Connection, LoadExtensionGuard, TransactionBehavior, Value, params_from_iter,
};

pub(crate) const CRSQLITE_LIB: &str = env!("CARGO_CDYLIB_FILE_CRSQLITE");

#[derive(Clone, Copy, Debug)]
pub(crate) enum Replica {
    Alice,
    Bob,
}

impl Replica {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Alice => "alice",
            Self::Bob => "bob",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Todo {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) completed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Change {
    table: String,
    pk: Vec<u8>,
    cid: String,
    val: Value,
    col_version: i64,
    db_version: i64,
    site_id: Vec<u8>,
    cl: i64,
    seq: i64,
}

impl Change {
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

pub(crate) type ChangeBatch = Vec<Change>;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReplicaState {
    todos: Vec<Todo>,
    db_version: i64,
    changes: ChangeBatch,
}

pub(crate) struct CrrConnection {
    connection: Connection,
}

impl CrrConnection {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let database = Builder::new_local(path)
            .build()
            .await
            .with_context(|| format!("open {}", path.display()))?;
        let connection = database
            .connect()
            .with_context(|| format!("connect to {}", path.display()))?;

        let guard = LoadExtensionGuard::new(&connection).context("enable extension loading")?;
        connection
            .load_extension(CRSQLITE_LIB, Some("sqlite3_crsqlite_init"))
            .with_context(|| format!("load {CRSQLITE_LIB}"))?;
        drop(guard);

        let site_id = single_value(&connection, "SELECT crsql_site_id()")
            .await
            .context("verify cr-sqlite")?;
        ensure!(
            matches!(site_id, Value::Blob(ref bytes) if !bytes.is_empty()),
            "crsql_site_id() did not return a nonempty blob"
        );

        connection
            .busy_timeout(Duration::from_secs(2))
            .context("set busy timeout")?;

        Ok(Self { connection })
    }

    pub(crate) async fn initialize(&self) -> Result<()> {
        let journal_mode = single_value(&self.connection, "PRAGMA journal_mode=WAL")
            .await
            .context("enable WAL")?;
        ensure!(
            matches!(journal_mode, Value::Text(ref mode) if mode.eq_ignore_ascii_case("wal")),
            "PRAGMA journal_mode=WAL returned {journal_mode:?}"
        );

        self.connection
            .execute(
                "CREATE TABLE todos (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL DEFAULT '', completed INTEGER NOT NULL DEFAULT 0)",
                (),
            )
            .await
            .context("create todos")?;
        single_value(&self.connection, "SELECT crsql_as_crr('todos')")
            .await
            .context("enable CRR for todos")?;
        Ok(())
    }

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

    pub(crate) async fn snapshot(&self) -> Result<Vec<Todo>> {
        read_todos(&self.connection).await
    }

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

    pub(crate) async fn changes(&self) -> Result<ChangeBatch> {
        read_changes(&self.connection).await
    }

    pub(crate) async fn apply(&self, changes: ChangeBatch) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin change batch")?;
        let statement = transaction
            .prepare("INSERT INTO crsql_changes VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")
            .await
            .context("prepare change insertion")?;

        for change in changes {
            statement
                .execute(params_from_iter(change.values()))
                .await
                .context("apply CRR change")?;
            statement.reset();
        }

        drop(statement);
        transaction.commit().await.context("commit change batch")?;
        Ok(())
    }

    pub(crate) async fn finalize(&self) -> Result<()> {
        single_value(&self.connection, "SELECT crsql_finalize()")
            .await
            .context("finalize cr-sqlite")?;
        Ok(())
    }
}

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

async fn read_db_version(connection: &Connection) -> Result<i64> {
    match single_value(connection, "SELECT crsql_db_version()").await? {
        Value::Integer(version) => Ok(version),
        value => anyhow::bail!("crsql_db_version() returned {value:?}"),
    }
}

async fn read_changes(connection: &Connection) -> Result<ChangeBatch> {
    let mut rows = connection
        .query(
            "SELECT \"table\", pk, cid, val, col_version, db_version, site_id, cl, seq FROM crsql_changes ORDER BY db_version, seq, \"table\", pk, cid, site_id",
            (),
        )
        .await?;
    let mut changes = Vec::new();
    while let Some(row) = rows.next().await? {
        changes.push(Change {
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
