use chunky_bits::file::hash::{
    DataHasher,
    DataVerifier,
    Sha256Hash,
};

const HELLO_PAYLOAD: &[u8] = "Hello World".as_bytes();
const HELLO_CHECKSUM: &str = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";

#[test]
fn sha256() {
    let hash = Sha256Hash::from_buf(&HELLO_PAYLOAD);
    let hash_str = format!("{}", hash);
    assert_eq!(
        hash_str,
        HELLO_CHECKSUM,
    );
    assert!(hash.verify(HELLO_PAYLOAD));
}

#[tokio::test]
async fn sha256_async() {
    let (hash, payload_recv) = Sha256Hash::from_buf_async(HELLO_PAYLOAD).await.unwrap();
    assert_eq!(HELLO_PAYLOAD, payload_recv);
    let hash_str = format!("{}", hash);
    assert_eq!(
        hash_str,
        HELLO_CHECKSUM,
    );
    assert!(hash.verify_async(HELLO_PAYLOAD).await.unwrap().0);
}
