use std::marker::Unpin;

use anyhow::Result;
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
    sync::mpsc,
};

pub async fn io_copy<R, W>(reader: &mut R, writer: &mut W) -> Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Send + Sync + Unpin + 'static,
{
    const BUF_SIZE: usize = 2 << 14;
    let mut data_a = [0; BUF_SIZE];
    let mut data_b = [0; BUF_SIZE];
    let mut last_used = data_b.as_ptr();

    let (tx, mut rx) = mpsc::channel::<&'static [u8]>(1);
    let writer: &'static mut W = unsafe { std::mem::transmute(writer) };
    let write_handle = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            writer.write_all(data).await.unwrap()
        }
    });

    let mut total_bytes: u64 = 0;

    loop {
        let data: &mut [u8] = if last_used.eq(&data_b.as_ptr()) {
            &mut data_a
        } else {
            &mut data_b
        };
        last_used = data.as_ptr();
        let tx = tx.reserve().await?;
        match reader.read(data).await? {
            0 => break,
            len => {
                let data: &[u8] = &data[0..len];
                let data: &'static [u8] = unsafe { std::mem::transmute(data) };
                tx.send(data);
                total_bytes += len as u64;
            },
        }
    }
    drop(tx);
    write_handle.await.unwrap();
    Ok(total_bytes)
}
