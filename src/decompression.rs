use crate::ObjectReader;
use async_compression::tokio::bufread::GzipDecoder;
use tokio::io::BufReader;

#[derive(Copy, Clone)]
pub enum Algorithm {
    Gzip,
    None,
}

// pub fn decompress_bufread<R>(buf_read: R, algorithm: Algorithm) -> Box<dyn BufRead + Send + Sync>
// where
//     R: BufRead + Send + Sync + 'static,
// {
//     let output_stream: Box<dyn BufRead + Send + Sync> = match algorithm {
//         Algorithm::Gzip => Box::new(BufReader::new(GzDecoder::new(buf_read))),
//         Algorithm::None => Box::new(buf_read),
//     };
//     output_stream
// }

pub async fn decompress_async_buf_read(
    async_buf_read: ObjectReader,
    algorithm: Algorithm,
) -> ObjectReader {
    let output_stream: ObjectReader = match algorithm {
        Algorithm::Gzip => Box::pin(BufReader::new(GzipDecoder::new(async_buf_read))),
        Algorithm::None => async_buf_read,
    };
    output_stream
}
