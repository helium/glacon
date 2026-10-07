//! Messages used by glacon's tests. Kept in its own crate so only tests need
//! `protoc`; the library itself never compiles protobuf files.

include!(concat!(env!("OUT_DIR"), "/glacon.test.rs"));
