// Executable entry point for lab0. It starts the Tokio runtime and
// delegates the experiment lifecycle to the `experiment` module.
mod db;
mod experiment;
mod tasks;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    experiment::run().await
}
