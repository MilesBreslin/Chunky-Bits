use std::{
    error::Error,
    ops::{
        Deref,
        DerefMut,
    },
};

use chunky_bits::{
    cluster::{
        Cluster,
        ClusterNode,
        MetadataPath,
        MetadataTypes,
    },
    error::{
        ClusterError,
        FileWriteError,
    },
    file::Location,
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
