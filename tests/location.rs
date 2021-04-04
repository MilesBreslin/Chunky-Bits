use std::{
    error::Error,
    str::FromStr,
};

use chunky_bits::file::Location;
use tempfile::tempdir;

#[tokio::test]
async fn location_read() -> Result<(), Box<dyn Error>> {
    let location = Location::from_str("/bin/sh")?;
    let bytes = location.read().await?;
    assert!(bytes.len() > 0);
    Ok(())
}

#[tokio::test]
async fn location_write() -> Result<(), Box<dyn Error>> {
    let payload = "Hello World".as_bytes();
    let dir = tempdir()?;
    let dir_location = Location::from(dir.path());
    let location = dir_location.write_subfile("TESTFILE", payload).await?;
    let payload_read = location.read().await?;
    dir.close()?;
    assert_eq!(
        format!("{}/TESTFILE", dir_location),
        format!("{}", location),
    );
    assert!(location.is_child_of(&dir_location));
    assert_eq!(payload, payload_read);
    Ok(())
}
