use std::path::PathBuf;

use structopt::StructOpt;
use tokio::io;
pub mod cluster_location;
pub mod config;
pub mod error_message;
use crate::{
    cluster_location::ClusterLocation,
    config::Config,
};

#[derive(StructOpt)]
struct Opt {
    /// Location for the config file
    #[structopt(long)]
    config: Option<PathBuf>,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    Cat {
        target: ClusterLocation,
    },
    ConfigInfo,
    ClusterInfo {
        cluster: String,
    },
    Cp {
        source: ClusterLocation,
        destination: ClusterLocation,
    },
    Ls {
        target: ClusterLocation,
    },
    Resilver {
        target: ClusterLocation,
    },
    Verify {
        target: ClusterLocation,
    },
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(_) => {},
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        },
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Opt { command, config } = Opt::from_args();
    let config = Config::builder().path(config);
    match command {
        Command::Cat { target } => {
            let config = config.load_or_default().await?;
            let mut reader = target.get_reader(&config).await?;
            io::copy(&mut reader, &mut io::stdout()).await?;
        },
        Command::ConfigInfo => {
            let config = config.load_or_default().await?;
            serde_yaml::to_writer(std::io::stdout(), &config)?;
        },
        Command::ClusterInfo { cluster } => {
            let config = config.load_or_default().await?;
            let cluster = config.get_cluster(&cluster).await?;
            serde_yaml::to_writer(std::io::stdout(), &cluster)?;
        },
        Command::Cp {
            source,
            destination,
        } => {
            let config = config.load_or_default().await?;
            let mut reader = source.get_reader(&config).await?;
            destination.write_from_reader(&config, &mut reader).await?;
        },
        Command::Ls { target } => {
            let config = config.load_or_default().await?;
            let files = target.list_files(&config).await?;
            for file in files {
                println!("{}", file);
            }
        },
        Command::Resilver { target } => {
            let config = config.load_or_default().await?;
            let report = target.resilver(&config).await?;
            println!("{}", *report);
        },
        Command::Verify { target } => {
            let config = config.load_or_default().await?;
            let report = target.verify(&config).await?;
            println!("{}", *report);
        },
    }
    Ok(())
}
