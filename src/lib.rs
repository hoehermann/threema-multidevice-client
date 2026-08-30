//! A lightweight, headless Threema client library.
//!
//! Links as an additional device in an existing multi-device group (the required secrets can be
//! extracted from an existing linked Threema Desktop installation, see [`Config`]) and processes
//! incoming plain-text messages. [`run`] drives the whole client; it currently prints messages to
//! stdout (moving output behind an event channel is the next step of the library conversion).
use core::cell::RefCell;
use std::{path::PathBuf, sync::Arc};

use anyhow::Context as _;
use libthreema::{
    cli::{FullIdentityConfig, FullIdentityConfigOptions},
    common::{ClientInfo, keys::DeviceGroupKey},
    csp_e2e::CspE2eProtocolContextInit,
    https::cli::https_client_builder,
    model::provider::{ProviderError, SettingsProvider, in_memory::DefaultShortcutProvider},
};
use tokio::sync::mpsc;

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

/// Everything needed to run the client.
pub struct Config {
    /// Identity, keys and server environment. Interim shape: this reuses libthreema's CLI options
    /// struct (all fields are plain `pub` data, no argument parsing involved); it will be replaced
    /// by fields owned by this crate once the embedding API settles.
    pub identity: FullIdentityConfigOptions,
    /// Directory holding this client's persistent state (`contacts.json`, `nonces.json`).
    pub state_dir: PathBuf,
}

/// Never blocks unknown identities: there's no UI here to review/accept them anyway.
struct AllowAllSettingsProvider;
impl SettingsProvider for AllowAllSettingsProvider {
    fn block_unknown_identities(&self) -> Result<bool, ProviderError> {
        Ok(false)
    }
}

/// Connects to the chat and mediator servers and processes messages until one of the connections
/// ends or fails. Does not reconnect and does not handle signals -- lifecycle policy is the
/// caller's.
///
/// The internals are `!Send` (single-task by design), so the returned future must be driven on a
/// current-thread runtime or `LocalSet`, not `tokio::spawn`ed onto a multi-threaded runtime.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let http_client = https_client_builder().build()?;
    let identity = FullIdentityConfig::from_options(&http_client, config.identity).await?;

    // Validated up front (rather than only via `d2m_context()` below) so the raw device group
    // key is available for decrypting D2D `Reflected` envelopes too.
    let d2x_config = identity.d2x_config.as_ref().context(
        "Multi-device must be configured: pass --d2x-device-id, --csp-device-id, \
         --device-group-key and --expected-device-slot-state",
    )?;

    let d2m_context = identity.d2m_context().context(
        "Multi-device must be configured: pass --d2x-device-id, --csp-device-id, \
         --device-group-key and --expected-device-slot-state",
    )?;

    // Held for the E2E driver's whole lifetime to decrypt D2D `Reflected` envelopes (see
    // `crate::d2d`) -- a separate instance from the one inside `d2m_context`, since
    // `DeviceGroupKey` isn't `Clone`.
    let device_group_key = DeviceGroupKey::from(&d2x_config.device_group_key);

    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("Failed to create {}", config.state_dir.display()))?;
    let nonces =
        NonceStore::load(config.state_dir.join("nonces.json")).context("Failed to load nonces.json")?;
    let contacts = ContactStore::load(config.state_dir.join("contacts.json"), identity.minimal.user_identity)
        .context("Failed to load contacts.json")?;

    let csp_e2e_context = CspE2eProtocolContextInit {
        client_info: ClientInfo::Libthreema,
        config: Arc::clone(&identity.minimal.common.config),
        csp_e2e: identity.csp_e2e_context_init(Box::new(RefCell::new(nonces.scoped(NonceScope::CspE2e)))),
        d2x: identity.d2x_context_init(Box::new(RefCell::new(nonces.scoped(NonceScope::D2x)))),
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
        identity
            .minimal
            .common
            .config
            .chat_server_address
            .addresses(identity.csp_server_group),
        identity
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
        csp_e2e_outgoing_tx.clone(),
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
    }

    Ok(())
}
