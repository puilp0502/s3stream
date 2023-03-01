mod decompression;

use crate::decompression::{decompress_async_buf_read, Algorithm};
use anyhow::{bail, Context, Result};
use argh::FromArgs;
use aws_sdk_s3::Client;
use flume::bounded;
use log::info;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncBufRead, AsyncRead};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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

    #[argh(option, short = 'D')]
    /// number of decompression tasks
    pub decompression_concurrency: Option<usize>,

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

pub type UnbufferedReader = Pin<Box<dyn AsyncRead + Send + Sync>>;
pub type ObjectReader = Pin<Box<dyn AsyncBufRead + Send + Sync>>;

pub struct PrefetchWorkerHandle {
    pub stop_flag: Arc<AtomicBool>,
    pub join_handle: JoinHandle<Result<ObjectReader>>,
}

impl PrefetchWorkerHandle {
    async fn resolve_object_reader(self) -> Result<ObjectReader> {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.join_handle.await.unwrap()
    }
}

pub async fn create_object_reader(
    client: Arc<Client>,
    bucket: String,
    key: String,
) -> Result<UnbufferedReader> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;
    let output_reader = resp.body.into_async_read();
    let output_reader: UnbufferedReader = Box::pin(output_reader);
    Ok(output_reader)
}

pub fn create_prefetch_worker(object_reader: UnbufferedReader) -> PrefetchWorkerHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let join_handle = tokio::spawn(prefetch_object_reader_worker(
        stop_flag.clone(),
        object_reader,
    ));
    PrefetchWorkerHandle {
        stop_flag,
        join_handle,
    }
}

// FIXME: This can be optimized to avoid double-copying
pub async fn prefetch_object_reader_worker(
    stop_flag: Arc<AtomicBool>,
    mut object_reader: UnbufferedReader,
) -> Result<ObjectReader> {
    let mut stash = vec![];
    while !stop_flag.load(Ordering::Relaxed) {
        let mut buf = vec![0 as u8; 64 * 1024];
        let read_bytes = object_reader.read(&mut buf).await?;
        stash.extend_from_slice(&buf[..read_bytes]);
        if read_bytes == 0 {
            break;
        }
    }
    let stash = Cursor::new(stash);

    let chained_reader = AsyncReadExt::chain(stash, object_reader);
    let chained_reader: ObjectReader = Box::pin(BufReader::new(chained_reader));
    Ok(chained_reader)
}

pub async fn receive_task_worker(
    client: Client,
    bucket: String,
    sem: Arc<Semaphore>,
    mut in_channel: Receiver<String>,
    out_channel: flume::Sender<(String, Result<(PrefetchWorkerHandle, OwnedSemaphorePermit)>)>,
) {
    let client = Arc::new(client);
    while let Some(key) = in_channel.recv().await {
        let permit_owned = sem.clone().acquire_owned().await.unwrap();
        info!("{} recv task created", key);
        let task = create_object_reader(client.clone(), bucket.clone(), key.clone()).await;
        let result = task.map(|reader| (create_prefetch_worker(reader), permit_owned));
        out_channel
            .send_async((key.clone(), result))
            .await
            .unwrap_or_else(|_| panic!("broken internal pipe"))
    }
}

pub async fn decompressor_worker(
    in_channel: flume::Receiver<(String, Result<(PrefetchWorkerHandle, OwnedSemaphorePermit)>)>,
    out_channel: Sender<(String, String)>,
    compression_algorithm: Algorithm,
) {
    while let Ok((key, result)) = in_channel.recv_async().await {
        info!("{} decompression started", key);
        match result {
            Ok((handle, permit)) => {
                let reader_result = handle.resolve_object_reader().await;
                match reader_result {
                    Ok(reader) => {
                        let decompressed_buf_read =
                            decompress_async_buf_read(reader, compression_algorithm).await;
                        let mut lines = decompressed_buf_read.lines();
                        while let Some(line) = lines.next_line().await.unwrap() {
                            out_channel
                                .send((key.clone(), line))
                                .await
                                .expect("broken internal pipe");
                        }
                        info!("{} decompression completed", key);
                    }
                    Err(e) => {
                        eprintln!("Error reading {}: {:?}", key, e);
                    }
                }
                drop(permit);
            }
            Err(e) => {
                eprintln!("Error reading {}: {:?}", key, e);
            }
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
    let decompression_concurrency = args.decompression_concurrency.unwrap_or(1);

    let semaphore = Arc::new(Semaphore::new(concurrency));

    let (key_tx, key_rx) = channel(1024);
    let (task_tx, task_rx) = bounded(1024);
    let (decompressed_tx, decompressed_rx) = channel(1024);
    let task_creator_handle = tokio::spawn(receive_task_worker(
        client,
        bucket.clone(),
        semaphore.clone(),
        key_rx,
        task_tx,
    ));
    let mut decompressor_handles = vec![];
    for _ in 0..decompression_concurrency {
        let task_rx = task_rx.clone();
        let decompressed_tx = decompressed_tx.clone();
        let compression_algorithm = compression_algorithm.clone();
        decompressor_handles.push(tokio::spawn(decompressor_worker(
            task_rx,
            decompressed_tx,
            compression_algorithm,
        )));
    }
    drop(decompressed_tx);
    let printer_handle = tokio::spawn(printer_worker(decompressed_rx));

    for key in list_result {
        key_tx.send(key).await.expect("broken internal pipe");
    }
    // close channel
    drop(key_tx);

    task_creator_handle.await.unwrap();
    futures::future::join_all(decompressor_handles).await;
    printer_handle.await.unwrap();

    Ok(())
}
