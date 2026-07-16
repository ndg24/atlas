fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=../../../proto/format.proto");
    prost_build::compile_protos(&["../../../proto/format.proto"], &["../../../proto"])
        .expect("compiling proto/format.proto");
}
