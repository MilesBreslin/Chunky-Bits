use std::{
    error::Error,
    num::NonZeroUsize,
    sync::Arc,
};

use chunky_bits::{
    error::FileWriteError,
    file::{
        FileReference,
        Location,
        VoidDestination,
        WeightedLocation,
    },
};
use tempfile::{
    tempdir,
    TempDir,
};
use tokio::io::{
    repeat,
    AsyncReadExt,
};

#[tokio::test]
async fn test_file_write() -> Result<(), Box<dyn Error>> {
    for data in 1..=3 {
        for parity in 1..=3 {
            // Set the length to be something not divisible by (d+p)==5
            let length = (1 << 23) + 7;
            let chunk_size = 1 << 20;
            let mut reader = repeat(0).take(length);
            let file_ref = FileReference::from_reader(
                &mut reader,
                Arc::new(VoidDestination),
                chunk_size,
                data,
                parity,
                NonZeroUsize::new(20).unwrap(),
            )
            .await?;
            assert_eq!(file_ref.length.unwrap(), length);
            let part_size: u64 = (chunk_size as u64) * (data as u64);
            assert_eq!(
                file_ref.parts.len() as u64,
                (length + part_size - 1) / part_size,
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn not_enough_writers() -> Result<(), Box<dyn Error>> {
    let mut directories: Vec<TempDir> = vec![tempdir()?, tempdir()?, tempdir()?, tempdir()?];
    let locations: Vec<WeightedLocation> = directories
        .iter()
        .map(|dir| Location::from(dir.path()).into())
        .collect();
    let locations = Arc::new(locations);
    for data in 1..=3 {
        for parity in 1..=3 {
            let length = 1;
            let chunk_size = 1 << 20;
            let mut reader = repeat(0).take(length);
            let result = FileReference::from_reader(
                &mut reader,
                locations.clone(),
                chunk_size,
                data,
                parity,
                NonZeroUsize::new(20).unwrap(),
            )
            .await;
            if (data + parity) > locations.len() {
                match result {
                    Err(FileWriteError::NotEnoughWriters) => {},
                    Ok(_) => {
                        panic!(
                            "Wrote {} chunks to {} locations",
                            (data + parity),
                            locations.len(),
                        );
                    },
                    Err(err) => {
                        panic!(
                            "Could not write {} chunks to {} locations: {}",
                            (data + parity),
                            locations.len(),
                            err,
                        );
                    },
                }
            } else if let Err(err) = result {
                panic!(
                    "Could not write {} chunks to {} locations: {}",
                    (data + parity),
                    locations.len(),
                    err,
                );
            }
        }
    }
    for dir in directories.drain(..) {
        dir.close()?;
    }
    Ok(())
}
