pub mod convert;

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod shared {
    tonic::include_proto!("shared");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod shredstream {
    tonic::include_proto!("shredstream");
}
