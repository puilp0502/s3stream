mod decompression;

use crate::decompression::{decompress_bufread, Algorithm};
use anyhow::{bail, Context, Result};
use argh::FromArgs;
use aws_sdk_s3::Client;
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use log::info;
use std::io::BufRead;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use url::Url;

#[derive(Debug)]
pub struct S3Uri {
    bucket_name: String,
    path: String,
}

#[derive(FromArgs, Debug)]
/// Cat multiple files from S3
pub struct Args {
    #[argh(option)]
    /// if present, only cat files starting with prefix
    pub prefix: Option<String>,

    #[argh(option, short = 'n')]
    /// number of concurrent requests to make
    pub concurrency: Option<usize>,

    #[argh(switch, short = 'm')]
    /// print output in multifile streaming format
    pub multifile: bool,

    #[argh(switch, short = 'z')]
    /// if true, decompress s3 objects using gzip algorithm
    pub gzip: bool,

    #[argh(positional)]
    pub s3_uri: String,
}

pub fn parse_s3_uri(s3_uri: &str) -> Result<S3Uri> {
    let parsed = Url::parse(s3_uri)?;
    if parsed.scheme() != "s3" {
        bail!("provided uri is not a S3 uri");
    }
    let Some(host) = parsed.host() else { bail!("bucket name is not present in the uri")};
    let path = parsed.path();
    let stripped_path = if let Some(relpath) = path.strip_prefix("/") {
        relpath
    } else {
        path
    };
    Ok(S3Uri {
        bucket_name: host.to_string(),
        path: stripped_path.to_string(),
    })
}

pub async fn list_s3_uri(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>> {
    let mut vec = vec![];
    let mut resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;
    for object in resp.contents().unwrap_or_default() {
        vec.push(object.key().unwrap().to_string());
    }
    while resp.is_truncated() {
        let next_continuation_token = resp
            .next_continuation_token()
            .context("response was marked truncated, but no continuation token is provided")?;
        resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .continuation_token(next_continuation_token)
            .send()
            .await?;
        for object in resp.contents().unwrap_or_default() {
            vec.push(object.key().unwrap().to_string());
        }
    }
    Ok(vec)
}

pub async fn read_objects(
    client: Arc<Client>,
    bucket: String,
    key: String,
) -> Result<Reader<Bytes>> {
    let reader = (|| async {
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let input_reader = resp.body.collect().await?.into_bytes().reader();
        Ok(input_reader)
    })()
    .await;
    reader
}

pub type ReceiveTaskJoinHandle = JoinHandle<Result<Reader<Bytes>>>;

pub async fn create_receive_task_worker(
    client: Client,
    bucket: String,
    mut in_channel: Receiver<String>,
    out_channel: Sender<(String, ReceiveTaskJoinHandle)>,
) {
    let client = Arc::new(client);
    while let Some(key) = in_channel.recv().await {
        info!("{} recv task created", key);
        let task = tokio::spawn(read_objects(client.clone(), bucket.clone(), key.clone()));
        out_channel
            .send((key.clone(), task))
            .await
            .expect("broken internal pipe");
    }
}

pub async fn resolve_receive_task_worker(
    mut in_channel: Receiver<(String, ReceiveTaskJoinHandle)>,
    out_channel: Sender<(String, Reader<Bytes>)>,
) {
    while let Some((key, join_handle)) = in_channel.recv().await {
        let result = join_handle.await.unwrap();
        info!("{} recv task resolved", key);
        match result {
            Ok(reader) => {
                out_channel
                    .send((key.clone(), reader))
                    .await
                    .expect("broken internal pipe");
            }
            Err(e) => {
                eprintln!("error reading {}: {:#?}", key, e);
            }
        }
    }
}

pub async fn decompressor_worker(
    mut in_channel: Receiver<(String, Reader<Bytes>)>,
    out_channel: Sender<(String, String)>,
    compression_algorithm: Algorithm,
) {
    while let Some((key, reader)) = in_channel.recv().await {
        let decompressed_buf_read = decompress_bufread(reader, compression_algorithm);
        for read_result in decompressed_buf_read.lines() {
            out_channel
                .send((key.clone(), read_result.unwrap()))
                .await
                .expect("broken internal pipe");
        }
    }
}

pub async fn printer_worker(mut in_channel: Receiver<(String, String)>) {
    while let Some((_, line)) = in_channel.recv().await {
        println!("{}", line);
    }
}

pub async fn entrypoint() -> Result<()> {
    env_logger::init();

    let args: Args = argh::from_env();
    let s3_uri = parse_s3_uri(&args.s3_uri)?;

    let sdk_config = aws_config::from_env().load().await;
    let client = Client::new(&sdk_config);

    let bucket = s3_uri.bucket_name;
    let prefix = format!("{}{}", s3_uri.path, args.prefix.unwrap_or_default());

    let list_result = list_s3_uri(&client, &bucket, &prefix).await?;

    eprintln!("{:#?}", list_result);
    eprintln!("Total count: {}", list_result.len());

    let compression_algorithm = if args.gzip {
        Algorithm::Gzip
    } else {
        Algorithm::None
    };

    let concurrency = args.concurrency.unwrap_or(4);

    let (key_tx, key_rx) = channel(1024);
    let (task_tx, task_rx) = channel(concurrency);
    let (result_tx, result_rx) = channel(1024);
    let (decompressed_tx, decompressed_rx) = channel(1024);
    let task_creator_handle = tokio::spawn(create_receive_task_worker(
        client,
        bucket.clone(),
        key_rx,
        task_tx,
    ));
    let task_resolver_handle = tokio::spawn(resolve_receive_task_worker(task_rx, result_tx));
    let decompressor_handle = tokio::spawn(decompressor_worker(
        result_rx,
        decompressed_tx,
        compression_algorithm,
    ));
    let printer_handle = tokio::spawn(printer_worker(decompressed_rx));

    for key in list_result {
        key_tx.send(key).await.expect("broken internal pipe");
    }

    let join_handle = futures::future::join_all(vec![
        task_creator_handle,
        task_resolver_handle,
        decompressor_handle,
        printer_handle,
    ]);

    join_handle.await;

    Ok(())
}
