use futures::stream::{
    self,
    Stream,
    StreamExt,
};
use tokio::io::{
    self,
    AsyncRead,
};
use tokio_util::io::StreamReader;

use crate::{
    error::FileReadError,
    file::FileReference,
};

pub struct FileReadBuilder {
    file: FileReference,
    buffer: usize,
}

impl FileReadBuilder {
    pub(super) fn new(file: FileReference) -> FileReadBuilder {
        FileReadBuilder { file, buffer: 5 }
    }

    pub fn stream_reader(self) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> {
        let FileReadBuilder { file, buffer } = self;
        let FileReference { parts, length, .. } = file;
        let mut bytes_remaining: u64 = length.unwrap();
        stream::iter(
            parts
                .into_iter()
                .map(|part| async move { part.read().await }),
        )
        .buffered(buffer)
        .map(move |res| match res {
            Ok(mut bytes) => {
                if bytes.len() as u64 > bytes_remaining {
                    bytes.drain((bytes_remaining as usize)..);
                }
                bytes_remaining -= bytes.len() as u64;
                Ok(bytes)
            },
            Err(err) => Err(err),
        })
    }

    pub fn reader(self) -> impl AsyncRead {
        StreamReader::new(self.stream_reader().map(|res| -> io::Result<_> {
            match res {
                Ok(bytes) => Ok(std::io::Cursor::new(bytes)),
                Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
            }
        }))
    }
}
