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
    file::{
        FileReference,
        LocationContext,
    },
};

pub struct FileReadBuilder {
    file: FileReference,
    buffer: usize,
    location_context: LocationContext,
}

impl FileReadBuilder {
    pub(super) fn new(file: FileReference) -> FileReadBuilder {
        FileReadBuilder {
            file,
            buffer: 5,
            location_context: Default::default(),
        }
    }

    pub fn location_context(self, location_context: LocationContext) -> FileReadBuilder {
        let mut new = self;
        new.location_context = location_context;
        new
    }

    pub fn stream_reader(self) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> {
        let FileReadBuilder {
            file,
            buffer,
            location_context,
        } = self;
        let FileReference { parts, length, .. } = file;
        let mut bytes_remaining: u64 = length.unwrap();
        stream::iter(parts.into_iter().map(move |part| {
            let location_context = location_context.clone();
            async move { part.read_with_context(&location_context).await }
        }))
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
