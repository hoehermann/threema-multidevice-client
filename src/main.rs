//! Command-line front-end for the client library: parses arguments, runs the client until it
//! terminates or Ctrl-C is received, and prints every reported message to stdout.
use std::path::PathBuf;

use clap::Parser;
use libthreema::{cli::FullIdentityConfigOptions, utils::logging::init_stderr_logging};
use tokio::sync::mpsc;
use tracing::Level;

use threema_cli::{Command, Conversation, Event, TextMessage};

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

/// `Name (IDENTITY)` when a name is known, else just the bare identity.
fn label(identity: &str, name: Option<&str>) -> String {
    match name {
        Some(name) => format!("{name} ({identity})"),
        None => identity.to_owned(),
    }
}

fn print_text_message(message: &TextMessage) {
    let TextMessage {
        timestamp_ms, text, ..
    } = message;
    if message.outgoing {
        let to = match &message.conversation {
            Conversation::Contact { identity, name } => {
                format!("{} [{identity}]", label(identity, name.as_deref()))
            },
            Conversation::Group {
                creator_identity,
                group_id,
            } => format!("group {creator_identity}/{group_id}"),
            Conversation::DistributionList { id } => format!("distribution list {id}"),
            Conversation::Unknown => "<unknown conversation>".to_owned(),
        };
        println!("[{timestamp_ms}] me (to {to}): {text}");
    } else {
        let sender_identity = message.sender_identity.as_deref().unwrap_or("<unknown>");
        let author = label(sender_identity, message.sender_name.as_deref());
        match &message.conversation {
            Conversation::Group {
                creator_identity,
                group_id,
            } => println!(
                "[{timestamp_ms}] {author} [{sender_identity}] (group {creator_identity}/{group_id}): {text}"
            ),
            _ => println!("[{timestamp_ms}] {author} [{sender_identity}]: {text}"),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arguments = Args::parse();
    init_stderr_logging(arguments.log_level);

    let config = threema_cli::Config {
        identity: arguments.config,
        state_dir: arguments.state_dir,
    };

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let printer = async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                Event::TextMessage(message) => print_text_message(&message),
            }
        }
    };

    // Ctrl-C requests a shutdown through the command channel rather than cancelling run() -- this
    // is the same path an embedder uses.
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Received Ctrl-C, shutting down");
            let _ = command_tx.send(Command::Shutdown);
        }
    });

    tokio::select! {
        result = threema_cli::run(config, event_tx, command_rx) => result?,
        () = printer => {},
    }

    Ok(())
}
