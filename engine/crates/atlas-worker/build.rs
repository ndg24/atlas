fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=../../../proto/worker.proto");
    tonic_build::configure()
        .compile_protos(&["../../../proto/worker.proto"], &["../../../proto"])
        .expect("compiling proto/worker.proto");
}
