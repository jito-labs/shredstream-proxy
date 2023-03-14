use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
                "protos/auth.proto",
                "protos/shared.proto",
                "protos/shredstream.proto",
                "protos/trace_shred.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
