use std::{
    error::Error,
    ops::{
        Deref,
        DerefMut,
    },
};

use chunky_bits::{
    cluster::{
        MetadataTypes,
        MetadataPath,
        Cluster,
    },
    file::{
        Location,
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

        // If the file changes, this should change, too
        assert_eq!(cluster.destinations.0.len(), 1);

        let ref mut node = cluster.destinations.0.first_mut().unwrap();
        node.location = Location::from(metadata_dir.path()).into();

        match cluster.metadata {
            MetadataTypes::Path(MetadataPath{ref mut path, ..}) => {
                *path = data_dir.path().to_owned();
            },
            #[allow(unreachable_patterns)]
            _ => panic!("Unexpected MetadataPath"),
        }

        Ok(TestCluster {
            cluster,
            data_dir,
            metadata_dir,
        })
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
    let cluster_profile = cluster.get_profile(None).unwrap();
    let mut reader = repeat(0).take(1);
    cluster.write_file("TESTFILE", &mut reader, cluster_profile, None).await?;
    Ok(())
}