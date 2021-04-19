use std::{
    collections::VecDeque,
    convert::TryFrom,
    error::Error,
    ops::{
        Deref,
        DerefMut,
    },
    sync::Arc,
};

use chunky_bits::{
    cluster::{
        sized_int::ChunkSize,
        Cluster,
        ClusterNode,
        MetadataPath,
        MetadataTypes,
    },
    error::{
        ClusterError,
        FileWriteError,
    },
    file::{
        FileReference,
        Location,
    },
};
use futures::stream;
use tempfile::{
    tempdir,
    TempDir,
};
use tokio::io::{
    self,
    repeat,
    AsyncRead,
    AsyncReadExt,
};

#[allow(dead_code)]
struct TestCluster {
    cluster: Cluster,
    // Kept for the drop implementation
    data_dir: TempDir,
    metadata_dir: TempDir,
}

impl Deref for TestCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl DerefMut for TestCluster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cluster
    }
}

impl TestCluster {
    async fn new() -> Result<TestCluster, Box<dyn Error>> {
        let metadata_dir = tempdir()?;
        let data_dir = tempdir()?;
        let mut cluster = Cluster::from_location("./examples/test.yaml").await?;

        match cluster.metadata {
            MetadataTypes::Path(MetadataPath { ref mut path, .. }) => {
                *path = data_dir.path().to_owned();
            },
            #[allow(unreachable_patterns)]
            _ => panic!("Unexpected MetadataPath"),
        }

        let mut cluster = TestCluster {
            cluster,
            data_dir,
            metadata_dir,
        };

        // If the file changes, this should change, too
        assert_eq!(cluster.destinations.0.len(), 1);

        cluster.get_node_mut().location = Location::from(cluster.metadata_dir.path()).into();

        Ok(cluster)
    }

    fn get_node_mut(&mut self) -> &mut ClusterNode {
        (*self).destinations.0.first_mut().unwrap()
    }

    fn default_reader() -> impl AsyncRead {
        tokio_util::io::StreamReader::new(stream::iter((0..80).map(|i: u8| {
            let bytes: VecDeque<u8> = ((0 as usize)..(1 << 8))
                .map(|x| (x % 128) as u8 + i)
                .collect();
            Ok::<_, io::Error>(bytes)
        })))
    }
}

#[tokio::test]
async fn test_cluster() -> Result<(), Box<dyn Error>> {
    TestCluster::new().await?;
    Ok(())
}

#[tokio::test]
async fn test_cluster_write() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::new().await?;
    let profile = cluster.get_profile(None).unwrap();
    let mut reader = repeat(0).take(1);
    cluster
        .write_file("TESTFILE", &mut reader, profile, None)
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cluster_not_enough_writers() -> Result<(), Box<dyn Error>> {
    let available_nodes = 2;
    let mut cluster = TestCluster::new().await?;
    // repeat 0 is still 1 usage of the node
    cluster.get_node_mut().repeat = available_nodes - 1;
    let profile = cluster.get_profile(None).unwrap();
    let mut reader = repeat(0).take(1);
    match cluster
        .write_file("TESTFILE", &mut reader, profile, None)
        .await
    {
        Err(ClusterError::FileWrite(FileWriteError::NotEnoughWriters)) => {},
        Ok(_) => panic!(
            "Wrote {} to {} nodes",
            (Into::<usize>::into(profile.data_chunks) + Into::<usize>::into(profile.parity_chunks)),
            available_nodes,
        ),
        Err(err) => panic!("Unexpected cluster write error: {}", err,),
    }
    Ok(())
}

#[tokio::test]
async fn test_resilver() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::new().await?;
    let mut profile = cluster.get_profile(None).unwrap().clone();
    profile.chunk_size = ChunkSize::try_from(10)?;
    let mut reader = TestCluster::default_reader();
    cluster
        .write_file("TESTFILE", &mut reader, &profile, None)
        .await?;
    let mut file_ref = cluster.get_file_ref("TESTFILE").await?;
    // File should be 100% Valid
    assert!(file_ref.verify().await.is_ok());
    let mut deleted_chunks: usize = 0;
    for part in file_ref.parts.iter_mut() {
        // Delete 1 / 3 data chunks
        let location_with_hash = part.data.first().unwrap();
        let location = location_with_hash.locations.first().unwrap();
        let _ = location.delete().await;
        assert!(location.read().await.is_err());

        // Delete 1 / 2 parity chunks
        let location_with_hash = part.parity.first().unwrap();
        let location = location_with_hash.locations.first().unwrap();
        let _ = location.delete().await;
        assert!(location.read().await.is_err());

        deleted_chunks += 2;
    }
    // File should not be 100% Valid
    let verify_report = file_ref.verify().await;
    assert!(!verify_report.is_ok());
    assert!(verify_report.is_available());
    assert!(verify_report.unavailable_locations().count() == deleted_chunks);

    let resilver_report = file_ref
        .resilver(Arc::new(cluster.get_destination(&profile)))
        .await;
    // All of the parts should report no errors during resilver
    assert!(resilver_report.is_ok());
    assert!(resilver_report.new_locations().count() == deleted_chunks);
    // File should be 100% Valid
    assert!(file_ref.verify().await.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_resilver_owned() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::new().await?;
    let mut profile = cluster.get_profile(None).unwrap().clone();
    profile.chunk_size = ChunkSize::try_from(10)?;
    let mut reader = TestCluster::default_reader();
    cluster
        .write_file("TESTFILE", &mut reader, &profile, None)
        .await?;
    let mut file_ref = cluster.get_file_ref("TESTFILE").await?;
    // File should be 100% Valid
    assert!(file_ref.verify().await.is_ok());
    let mut deleted_chunks: usize = 0;
    for part in file_ref.parts.iter_mut() {
        // Delete 1 / 3 data chunks
        let location_with_hash = part.data.first().unwrap();
        let location = location_with_hash.locations.first().unwrap();
        let _ = location.delete().await;
        assert!(location.read().await.is_err());

        // Delete 1 / 2 parity chunks
        let location_with_hash = part.parity.first().unwrap();
        let location = location_with_hash.locations.first().unwrap();
        let _ = location.delete().await;
        assert!(location.read().await.is_err());

        deleted_chunks += 2;
    }
    // File should not be 100% Valid
    let verify_report = file_ref.verify().await;
    assert!(!verify_report.is_ok());
    assert!(verify_report.is_available());
    assert!(verify_report.unavailable_locations().count() == deleted_chunks);

    let resilver_report = file_ref
        .resilver_owned(Arc::new(cluster.get_destination(&profile)))
        .await;
    // All of the parts should report no errors during resilver
    assert!(resilver_report.is_ok());
    assert!(resilver_report.new_locations().count() == deleted_chunks);
    Ok(())
}

#[tokio::test]
async fn test_profiler() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::new().await?;
    let mut reader = TestCluster::default_reader();
    let profile = cluster.get_profile(None).unwrap().clone();
    let (reporter, destination) = cluster.get_destination_with_profiler(&profile);
    FileReference::write_builder()
        .destination(destination)
        .data_chunks(2)
        .parity_chunks(1)
        .write(&mut reader)
        .await?;
    let report = reporter.profile().await;
    let average_duration = report.average_write_duration().unwrap();
    assert!(average_duration.as_nanos() > 0);
    assert!(average_duration.as_millis() < 1000);
    assert!(report.average_read_duration() == None);
    Ok(())
}
