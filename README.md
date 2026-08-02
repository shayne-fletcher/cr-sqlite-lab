<p align="center">
  <img src="./images/logo.jpeg" width="300" alt="cr-sqlite-lab logo">
</p>

<h1 align="center">cr-sqlite-lab</h1>

<p align="center">
  consumer experiments for the Cargo-native cr-sqlite extension
</p>

<p align="center">
  <a href="https://github.com/shayne-fletcher/cr-sqlite-lab/actions/workflows/ci.yml">
    <img src="https://github.com/shayne-fletcher/cr-sqlite-lab/actions/workflows/ci.yml/badge.svg" alt="repository checks">
  </a>
  <a href="https://shayne-fletcher.github.io/cr-sqlite-lab/">
    <img src="https://img.shields.io/badge/docs-github.io-blue" alt="docs">
  </a>
  <a href="./LICENSE">
    <img src="https://img.shields.io/badge/license-BSD--3--Clause-blue" alt="BSD-3-Clause license">
  </a>
</p>

A consumer and experiment project for cr-sqlite.

The current experiment builds the Cargo-native `cr-sqlite` dynamic library as an artifact dependency, loads it into every stock Rust libSQL connection, and proves CRR convergence between two independently written database files.

Alice and Bob each have an application task and a synchronizer task. Each task owns its own connection, and the synchronizers exchange `crsql_changes` batches through bounded channels without accessing one another's database.

Run the experiment with:

```sh
cargo run --example lab0
```

A successful run verifies the expected merged rows on both replicas and confirms that continued replay is idempotent.
