//! Command-line front-end for the client library: parses arguments and runs the client until it
//! terminates or Ctrl-C is received. Message output currently comes from the library itself
//! (stdout); see `threema_cli::run`.
use std::path::PathBuf;

use clap::Parser;
use libthreema::{cli::FullIdentityConfigOptions, utils::logging::init_stderr_logging};
use tracing::Level;

#[derive(Parser)]
#[command(about = "Prints incoming Threema plain-text messages to stdout")]
struct Args {
    #[command(flatten)]
    config: FullIdentityConfigOptions,

    /// Directory holding this client's persistent state (`contacts.json`, `nonces.json`).
    #[arg(long, default_value = ".")]
    state_dir: PathBuf,

    /// Logger verbosity (error, warn, info, debug, trace).
    #[arg(long, default_value = "warn")]
    log_level: Level,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arguments = Args::parse();
    init_stderr_logging(arguments.log_level);

    let config = threema_cli::Config {
        identity: arguments.config,
        state_dir: arguments.state_dir,
    };

    tokio::select! {
        result = threema_cli::run(config) => result?,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl-C, shutting down");
        },
    }

    Ok(())
}
