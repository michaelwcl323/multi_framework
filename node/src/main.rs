// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters};
use consensus::Consensus;
use env_logger::Env;
use primary::{CommittedBatch, Primary};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::watch;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Parallel PBFT instances with independent per-instance delivery.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(
                    SubCommand::with_name("primary")
                        .about("Run a complete node with direct batch proposals"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs a complete authority in one process.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // The legacy address map supplies client ingress on the primary host.
    for authority in committee.authorities.values() {
        anyhow::ensure!(
            authority.workers.len() == 1 && authority.workers.contains_key(&0),
            "PBFT nodes require exactly one client endpoint (workers entry 0)"
        );
        let addresses = &authority.workers[&0];
        let host = authority.primary.primary_to_primary.ip();
        anyhow::ensure!(
            addresses.transactions.ip() == host,
            "Transaction endpoint must be on the primary's host"
        );
    }

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels committed batches with independent (instance, sequence) positions.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // The primary command runs all authority tasks in this process.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_decisions, rx_decisions) = channel(CHANNEL_CAPACITY);
            let (tx_epoch, rx_epoch) = watch::channel(0);
            let epoch_length = parameters
                .epoch_length(committee.size())
                .context("Invalid segment length or epoch length overflow")?;
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store.clone(),
                tx_decisions,
                rx_epoch,
            );
            Consensus::spawn(committee, epoch_length, tx_epoch, rx_decisions, tx_output);
        }

        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives the contiguous global log across segments and epochs.
async fn analyze(mut rx_output: Receiver<CommittedBatch>) {
    while let Some(_decision) = rx_output.recv().await {
        // NOTE: Here goes the application logic.
    }
}
