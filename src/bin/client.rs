use anyhow::Result;
use simplekv::{CommandRequest, ProstClientStream, TlsClientConnector};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:6000";
    let ca_cert = include_str!("../../fixtures/ca.cert");
    let connector = TlsClientConnector::new("demo.simplekv.cc", None, Some(ca_cert))?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    let mut client = ProstClientStream::new(stream);
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());
    let data = client.execute(cmd).await?;
    info!("Got response {:?}", data);

    Ok(())
}
