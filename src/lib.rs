mod decompression;

use crate::decompression::{decompress_bufread, Algorithm};
use anyhow::{bail, Context, Result};
use argh::FromArgs;
use aws_sdk_s3::Client;
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use futures::{stream, StreamExt, TryStreamExt};
use std::io::BufRead;
use std::sync::Arc;
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
) -> (String, Result<Reader<Bytes>>) {
    let reader = (|| async {
        let resp = client.get_object().bucket(bucket).key(&key).send().await?;
        let input_reader = resp.body.collect().await?.into_bytes().reader();
        Ok(input_reader)
    })()
    .await;
    (key, reader)
}

pub fn decompress_and_print_objects(reader: Reader<Bytes>, compression_algorithm: Algorithm) {
    let output_stream = decompress_bufread(reader, compression_algorithm);

    for line in output_stream.lines() {
        if let Ok(line) = line {
            println!("{}", line);
        }
    }
}
pub async fn entrypoint() -> Result<()> {
    let args: Args = argh::from_env();
    let s3_uri = parse_s3_uri(&args.s3_uri)?;

    let sdk_config = aws_config::from_env().load().await;
    let client = Arc::new(Client::new(&sdk_config));

    let prefix = format!("{}{}", s3_uri.path, args.prefix.unwrap_or_default());

    let list_result = list_s3_uri(&client, &s3_uri.bucket_name, &prefix).await?;

    eprintln!("{:#?}", list_result);
    eprintln!("Total count: {}", list_result.len());

    let compression_algorithm = if args.gzip {
        Algorithm::Gzip
    } else {
        Algorithm::None
    };

    let key_stream = stream::iter(list_result);
    let mut object_bodies = key_stream
        .map(|key| {
            tokio::spawn(read_objects(
                client.clone(),
                s3_uri.bucket_name.clone(),
                key,
            ))
        })
        .buffered(4);

    while let Some(join_handle) = object_bodies.next().await {
        let (key, result) = join_handle.unwrap();
        match result {
            Ok(reader) => {
                decompress_and_print_objects(reader, compression_algorithm);
            }
            Err(e) => {
                eprintln!("error reading object \"{}\": {:?}", key, e);
            }
        }
    }

    Ok(())
}
