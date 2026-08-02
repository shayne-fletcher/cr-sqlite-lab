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
</p>

A consumer and experiment project for cr-sqlite.

The lab should load a cr-sqlite artifact the way an application would.
Changes required to build or package the extension belong in the
adjacent `~/project/cr-sqlite` checkout.

The first experiment will use stock Rust libSQL to open a local
database, load the extension from an explicit path, and exercise basic
CRR operations.
