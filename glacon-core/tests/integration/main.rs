//! Routes against in-memory inputs, middlewares and outputs, each with its
//! ledger in a temp dir: no services needed. One file per part of a route,
//! and one for the route as a whole.

mod common;
mod input;
mod middleware;
mod middleware_dedup;
mod output;
mod route;
