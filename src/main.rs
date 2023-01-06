use s3stream::entrypoint;

#[tokio::main]
async fn main() {
    entrypoint().await.unwrap();
}
