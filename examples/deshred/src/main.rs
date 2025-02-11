extern crate jito_protos;

use jito_protos::deshred::{deshred_service_client::DeshredServiceClient, DeshredRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std       ::error::Error>> {
    let mut client = DeshredServiceClient::connect("http://127.0.0.1:50051").await.unwrap();
    let mut stream = client.subscribe_to_entries(DeshredRequest {}).await.unwrap().into_inner();
    while let Some(entry) = stream.message().await.unwrap() {
        println!("Received entry: {:?}", entry);
    }
    Ok(())
}

