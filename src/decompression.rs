use flate2::bufread::GzDecoder;
use std::io::*;

#[derive(Copy, Clone)]
pub enum Algorithm {
    Gzip,
    None,
}

pub fn decompress_bufread<R>(buf_read: R, algorithm: Algorithm) -> Box<dyn BufRead>
where
    R: BufRead + 'static,
{
    let output_stream: Box<dyn BufRead> = match algorithm {
        Algorithm::Gzip => Box::new(BufReader::new(GzDecoder::new(buf_read))),
        Algorithm::None => Box::new(buf_read),
    };
    output_stream
}
