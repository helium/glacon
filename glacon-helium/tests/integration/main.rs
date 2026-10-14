//! Integration tests. Need the docker compose services:
//!
//!   docker compose up -d --wait

mod common;
mod crash;
mod end_to_end;
mod middleware;
mod runtime;
mod state;
