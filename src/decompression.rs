use flate2::bufread::GzDecoder;
use std::io::*;

#[derive(Copy, Clone)]
pub enum Algorithm {
    Gzip,
    None,
}

pub fn decompress_bufread<R>(buf_read: R, algorithm: Algorithm) -> Box<dyn BufRead + Send + Sync>
where
    R: BufRead + Send + Sync + 'static,
{
    let output_stream: Box<dyn BufRead + Send + Sync> = match algorithm {
        Algorithm::Gzip => Box::new(BufReader::new(GzDecoder::new(buf_read))),
        Algorithm::None => Box::new(buf_read),
    };
    output_stream
}
