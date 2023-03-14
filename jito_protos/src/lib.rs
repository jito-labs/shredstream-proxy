pub mod shared {
    tonic::include_proto!("shared");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

pub mod trace_shred {
    tonic::include_proto!("trace_shred");
}
