#[cfg(feature = "web_admin")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // use std::net::SocketAddr;
    // kuramoto_db::plugins::web_admin::run_standalone("127.0.0.1:8080".parse::<SocketAddr>().unwrap())
    //     .await
    Ok(())
}

#[cfg(not(feature = "web_admin"))]
fn main() {
    eprintln!("Enable the 'web_admin' feature: cargo run --example web_admin --features web_admin");
}
