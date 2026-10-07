fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/events.proto");
    prost_build::compile_protos(&["proto/events.proto"], &["proto"])
}
