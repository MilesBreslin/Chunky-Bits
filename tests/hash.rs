use chunky_bits::file::hash::Sha256Hash;
use futures::stream::{
    StreamExt,
    FuturesOrdered,
};

#[test]
fn sha256() {
    let payload = "Hello World".as_bytes();
    let hash = Sha256Hash::from_buf(&payload);
    let hash_str = format!("{}", hash);
    assert_eq!(
        hash_str,
        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
    );
}

#[tokio::test]
async fn sha256_async_singlethread() {
    sha256_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn sha256_async_multithread() {
    sha256_async().await;
}

async fn sha256_async() {
    // Generate hashes and bufs safely
    let mut orig_items: Vec<(Sha256Hash, Vec<u8>)> = (0..25)
        .map(|i| tokio::task::spawn_blocking(move || {
            let buf: Vec<u8> = (0..(1 << 22)).map(|_: usize| i).collect();
            let hash = Sha256Hash::from_buf(&buf);
            (hash, buf)
        }))
        .collect::<FuturesOrdered<_>>()
        .map(Result::unwrap)
        .collect()
        .await;
    // Ensure that the async version is correct
    let async_items = orig_items
        .drain(..)
        .map(|(hash, buf)| tokio::task::spawn(async move {
            // This is the function that contains unsafe code being tested
            let async_hash = Sha256Hash::from_buf_async(buf.as_slice()).await;
            assert_eq!(hash, async_hash);
        }))
        .collect::<FuturesOrdered<_>>()
        .map(Result::unwrap)
        .collect::<Vec<()>>()
        .await;
}