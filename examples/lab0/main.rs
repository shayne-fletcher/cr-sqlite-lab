mod db;
mod experiment;
mod tasks;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    experiment::run().await
}
