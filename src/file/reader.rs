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
    seek: u64,
    take: u64,
}

impl<T> FileReadBuilder<T> {
    pub(super) fn new(file: T) -> FileReadBuilder<T> {
        FileReadBuilder {
            file,
            buffer: 5,
            location_context: Default::default(),
            seek: 0,
            take: 0,
        }
    }

    pub fn seek(mut self, seek: u64) -> Self {
        self.seek = seek;
        self
    }

    pub fn take(mut self, take: u64) -> Self {
        self.take = take;
        self
    }

    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer;
        self
    }

    pub fn location_context(self, location_context: LocationContext) -> FileReadBuilder<T> {
        let mut new = self;
        new.location_context = location_context;
        new
    }

    fn inner_stream_reader<'a>(
        &self,
        parts: impl Iterator<Item = impl Borrow<FilePart> + Send + Sync> + Send + 'a,
        length: u64,
    ) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + Send + 'a {
        let FileReadBuilder {
            buffer,
            location_context,
            seek,
            take,
            ..
        } = self;
        let mut bytes_remaining: u64 = match take {
            0 => length - *seek,
            _ if length > *take => *take,
            _ => length,
        };
        let location_context = location_context.clone();
        let mut seek = *seek;
        stream::iter(parts.filter_map(move |part| {
            let location_context = location_context.clone();
            let mut read_skip: usize = 0;
            if seek != 0 {
                let part_len = part.borrow().len_bytes() as u64;
                if seek >= part_len {
                    seek -= part_len;
                    return None;
                }
                read_skip = seek as usize;
                seek = 0;
            }
            Some(async move {
                let mut bytes = part.borrow().read_with_context(&location_context).await?;
                if bytes.len() < read_skip {
                    bytes.drain(0..read_skip);
                    Ok(bytes)
                } else {
                    Ok(vec![])
                }
            })
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
        stream: impl Stream<Item = Result<Vec<u8>, FileReadError>> + Send + 'a,
    ) -> impl AsyncRead + Send + 'a {
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
    T: Borrow<FileReference> + Send + Sync,
{
    pub fn buffer_bytes(mut self, bytes: usize) -> Self {
        if let Some(part_len) = self.file.borrow().parts.first().map(FilePart::len_bytes) {
            self.buffer = (bytes + (part_len / 2)) / part_len;
            if self.buffer == 0 {
                self.buffer = 1;
            }
        }
        self
    }
    pub fn stream_reader(&self) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + Send + '_ {
        let FileReference { parts, length, .. } = self.file.borrow();
        self.inner_stream_reader(parts.iter(), length.unwrap())
    }

    pub fn reader(&self) -> impl AsyncRead + Send + Unpin + '_ {
        Self::inner_reader(self.stream_reader())
    }
}

impl<T> FileReadBuilder<T>
where
    T: Into<FileReference> + Send + 'static,
{
    pub fn stream_reader_owned(
        self,
    ) -> impl Stream<Item = Result<Vec<u8>, FileReadError>> + Send + 'static {
        let FileReadBuilder {
            buffer,
            location_context,
            file,
            seek,
            take,
        } = self;
        let new = FileReadBuilder {
            buffer,
            location_context,
            file: (),
            seek,
            take,
        };
        let file = file.into();
        let length = file.length.unwrap();
        let parts = file.parts.into_iter();
        new.inner_stream_reader(parts, length)
    }

    pub fn reader_owned(self) -> impl AsyncRead + Send + Unpin + 'static {
        FileReadBuilder::<()>::inner_reader(self.stream_reader_owned())
    }
}
