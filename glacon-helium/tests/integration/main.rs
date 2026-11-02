//! Integration tests, one file per part of glacon-helium. Need the docker
//! compose services:
//!
//!   docker compose up -d --wait

mod common;
mod file_store_input;
mod iceberg_output;
mod middleware;
