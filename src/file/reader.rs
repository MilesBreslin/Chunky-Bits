use std::borrow::Borrow;

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
        FilePart,
        FileReference,
        LocationContext,
    },
};

pub struct FileReadBuilder<T> {
    file: T,
    buffer: usize,
    location_context: LocationContext,
}

impl<T> FileReadBuilder<T> {
    pub(super) fn new(file: T) -> FileReadBuilder<T> {
        FileReadBuilder {
            file,
            buffer: 5,
            location_context: Default::default(),
        }
    }

    pub fn location_context(self, location_context: LocationContext) -> FileReadBuilder<T> {
        let mut new = self;
        new.location_context = location_context;
        new
    }

    fn inner_stream_reader<'a>(
        &self,
        parts: impl Iterator<Item = impl Borrow<FilePart>> + 'a,
        length: u64,
    ) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + 'a {
        let FileReadBuilder {
            buffer,
            location_context,
            ..
        } = self;
        let mut bytes_remaining: u64 = length;
        let location_context = location_context.clone();
        stream::iter(parts.map(move |part| {
            let location_context = location_context.clone();
            async move { part.borrow().read_with_context(&location_context).await }
        }))
        .buffered(*buffer)
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

    fn inner_reader<'a>(
        stream: impl Stream<Item = Result<Vec<u8>, FileReadError>> + 'a,
    ) -> impl AsyncRead + 'a {
        StreamReader::new(stream.map(|res| -> io::Result<_> {
            match res {
                Ok(bytes) => Ok(std::io::Cursor::new(bytes)),
                Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
            }
        }))
    }
}

impl<T> FileReadBuilder<T>
where
    T: Borrow<FileReference>,
{
    pub fn stream_reader(&self) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + '_ {
        let FileReference { parts, length, .. } = self.file.borrow();
        self.inner_stream_reader(parts.iter(), length.unwrap())
    }

    pub fn reader(&self) -> impl AsyncRead + Unpin + '_ {
        Self::inner_reader(self.stream_reader())
    }
}

impl<T> FileReadBuilder<T>
where
    T: Into<FileReference> + 'static,
{
    pub fn stream_reader_owned(
        self,
    ) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + 'static {
        let FileReadBuilder {
            buffer,
            location_context,
            file,
        } = self;
        let new = FileReadBuilder {
            buffer,
            location_context,
            file: (),
        };
        let file = file.into();
        let length = file.length.unwrap();
        let parts = file.parts.into_iter();
        new.inner_stream_reader(parts, length)
    }

    pub fn reader_owned(self) -> impl AsyncRead + Unpin + 'static {
        FileReadBuilder::<()>::inner_reader(self.stream_reader_owned())
    }
}
