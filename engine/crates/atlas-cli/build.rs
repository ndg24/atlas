fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=../../../proto/catalog.proto");
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["../../../proto/catalog.proto"], &["../../../proto"])
        .expect("compiling proto/catalog.proto");
}
