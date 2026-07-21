//! A lightweight, headless Threema client.
//!
//! Links as an additional device in an existing multi-device group (see the `--csp-*`/`--d2x-*`
//! flags -- their values can be extracted from an existing linked Threema Desktop installation)
//! and prints incoming plain-text messages (timestamp, author, body) to stdout.
use core::cell::RefCell;
use std::{path::PathBuf, sync::Arc};

use anyhow::Context as _;
use clap::Parser;
use data_encoding::HEXLOWER_PERMISSIVE;
use libthreema::{
    cli::{FullIdentityConfig, FullIdentityConfigOptions},
    common::ClientInfo,
    csp_e2e::CspE2eProtocolContextInit,
    https::cli::https_client_builder,
    model::provider::{ProviderError, SettingsProvider, in_memory::DefaultShortcutProvider},
    utils::logging::init_stderr_logging,
};
use tokio::sync::mpsc;
use tracing::Level;

mod conversation;
mod csp;
mod d2d;
mod d2m;
mod e2e;
mod store;

use conversation::PrintingConversationProvider;
use csp::{CspProtocolRunner, PayloadQueuesForCspE2e};
use d2m::D2mProtocolRunner;
use e2e::CspE2eProtocolRunner;
use store::{ContactStore, NonceScope, NonceStore};

#[derive(Parser)]
#[command(about = "Prints incoming Threema plain-text messages to stdout")]
struct Args {
    #[command(flatten)]
    config: FullIdentityConfigOptions,

    /// Directory holding this client's persistent state (`contacts.json`, `nonces.json`).
    #[arg(long, default_value = ".")]
    state_dir: PathBuf,
}

/// Never blocks unknown identities: there's no UI here to review/accept them anyway.
struct AllowAllSettingsProvider;
impl SettingsProvider for AllowAllSettingsProvider {
    fn block_unknown_identities(&self) -> Result<bool, ProviderError> {
        Ok(false)
    }
}

/// Re-extracts a `--flag=value`/`--flag value` argument's raw string from argv.
///
/// Only used for `--device-group-key`: libthreema's `RawDeviceGroupKey` has no public byte
/// accessor (only `from_hex`), but decrypting D2D `Reflected` envelopes (`crate::d2d`) needs the
/// raw 32 bytes directly, so this re-reads argv rather than going through clap's parsed type.
fn find_arg_value(flag: &str) -> Option<String> {
    let prefix = format!("{flag}=");
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if let Some(value) = arg.strip_prefix(&prefix) {
            return Some(value.to_owned());
        }
        if arg == flag {
            return args.next();
        }
    }
    None
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    init_stderr_logging(Level::INFO);

    let http_client = https_client_builder().build()?;
    let arguments = Args::parse();
    let config = FullIdentityConfig::from_options(&http_client, arguments.config).await?;

    let d2m_context = config.d2m_context().context(
        "Multi-device must be configured: pass --d2x-device-id, --csp-device-id, \
         --device-group-key and --expected-device-slot-state",
    )?;

    // See `find_arg_value`'s doc comment for why this isn't just read off `d2x_config` directly.
    let device_group_key: [u8; 32] = {
        let hex = find_arg_value("--device-group-key")
            .context("--device-group-key missing (should have been rejected by clap already)")?;
        let bytes = HEXLOWER_PERMISSIVE
            .decode(hex.as_bytes())
            .context("--device-group-key is not valid hex (should have been rejected by clap already)")?;
        bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| anyhow::anyhow!("--device-group-key must be 32 bytes, got {}", bytes.len()))?
    };

    std::fs::create_dir_all(&arguments.state_dir)
        .with_context(|| format!("Failed to create {}", arguments.state_dir.display()))?;
    let nonces = NonceStore::load(arguments.state_dir.join("nonces.json"))
        .context("Failed to load nonces.json")?;
    let contacts = ContactStore::load(arguments.state_dir.join("contacts.json"), config.minimal.user_identity)
        .context("Failed to load contacts.json")?;

    let csp_e2e_context = CspE2eProtocolContextInit {
        client_info: ClientInfo::Libthreema,
        config: Arc::clone(&config.minimal.common.config),
        csp_e2e: config.csp_e2e_context_init(Box::new(RefCell::new(nonces.scoped(NonceScope::CspE2e)))),
        d2x: config.d2x_context_init(Box::new(RefCell::new(nonces.scoped(NonceScope::D2x)))),
        shortcut: Box::new(DefaultShortcutProvider),
        settings: Box::new(RefCell::new(AllowAllSettingsProvider)),
        contacts: Box::new(RefCell::new(contacts.clone())),
        conversations: Box::new(RefCell::new(PrintingConversationProvider::new(contacts.clone()))),
    };

    // Channels between the CSP connection and the E2E driver.
    let (csp_e2e_incoming_tx, csp_e2e_incoming_rx) = mpsc::channel(4);
    let (csp_e2e_outgoing_tx, csp_e2e_outgoing_rx) = mpsc::channel(4);

    // Channels between the D2M connection and the E2E driver (transactions + reflection).
    let (d2m_incoming_tx, d2m_incoming_rx) = mpsc::channel(16);
    let (d2m_outgoing_tx, d2m_outgoing_rx) = mpsc::channel(16);

    tracing::info!("Connecting to chat server");
    let (csp_runner, client_hello) = CspProtocolRunner::new(
        config
            .minimal
            .common
            .config
            .chat_server_address
            .addresses(config.csp_server_group),
        config
            .csp_context_init()
            .try_into()
            .context("CSP configuration should be valid")?,
    )
    .await?;

    tracing::info!("Connecting to mediator server");
    let d2m_runner = D2mProtocolRunner::new(d2m_context).await?;

    let e2e_runner = CspE2eProtocolRunner::new(
        http_client,
        csp_e2e_context,
        d2m_outgoing_tx,
        d2m_incoming_rx,
        device_group_key,
        contacts,
    );

    // None of these runners are `Send` (the in-memory providers use `Rc`), so they must run
    // concurrently within a single task via `select!` rather than via `tokio::spawn`.
    tokio::select! {
        result = csp_runner.run(client_hello, csp_e2e_incoming_tx, csp_e2e_outgoing_rx) => {
            result.context("CSP connection ended")?;
        },
        result = d2m_runner.run(d2m_incoming_tx, d2m_outgoing_rx) => {
            result.context("D2M connection ended")?;
        },
        result = e2e_runner.run(PayloadQueuesForCspE2e {
            incoming: csp_e2e_incoming_rx,
            outgoing: csp_e2e_outgoing_tx,
        }) => {
            result.context("Message processing ended")?;
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl-C, shutting down");
        },
    }

    Ok(())
}
