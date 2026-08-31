//! A lightweight, headless Threema client library.
//!
//! Links as an additional device in an existing multi-device group (the required secrets can be
//! extracted from an existing linked Threema Desktop installation, see [`Config`]) and processes
//! incoming plain-text messages. [`run`] drives the whole client and reports everything through
//! the [`Event`] channel handed to it.
use core::cell::RefCell;
use std::{path::PathBuf, sync::Arc};

use anyhow::Context as _;
use libthreema::{
    cli::{FullIdentityConfig, FullIdentityConfigOptions},
    common::{ClientInfo, keys::{ClientKey, DeviceGroupKey}},
    csp_e2e::CspE2eProtocolContextInit,
    https::cli::https_client_builder,
    model::provider::{ProviderError, SettingsProvider, in_memory::DefaultShortcutProvider},
};
use tokio::sync::mpsc;

mod command;
mod conversation;
mod csp;
mod d2d;
mod d2m;
mod e2e;
mod event;
mod store;

pub use command::{Command, Recipient};
pub use event::{Conversation, Event, TextMessage};
// Handy for embedders (the CLI, an FFI shim) that want log output without owning a tracing
// subscriber of their own.
pub use libthreema::utils::logging::init_stderr_logging;
// Re-exported so embedders can construct [`Config::identity`] (and parse its value types from
// strings) without managing their own libthreema path dependency.
pub use libthreema;

use conversation::EventConversationProvider;
use csp::{CspProtocolRunner, PayloadQueuesForCspE2e};
use d2m::D2mProtocolRunner;
use e2e::{CspE2eProtocolRunner, CspE2eRunnerDeps};
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
/// ends or fails, [`Command::Shutdown`] arrives (or the command sender is dropped), reporting
/// through `events` along the way. Does not reconnect and does not handle signals -- lifecycle
/// policy is the caller's.
///
/// The internals are `!Send` (single-task by design), so the returned future must be driven on a
/// current-thread runtime or `LocalSet`, not `tokio::spawn`ed onto a multi-threaded runtime.
/// [`Event`]s and [`Command`]s are plain data, so the other ends of both channels may live on any
/// thread.
pub async fn run(
    config: Config,
    events: mpsc::UnboundedSender<Event>,
    commands: mpsc::UnboundedReceiver<Command>,
) -> anyhow::Result<()> {
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
        conversations: Box::new(RefCell::new(EventConversationProvider::new(
            contacts.clone(),
            events.clone(),
        ))),
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

    // Both runners complete their handshakes inside `new`, so the connections are up now.
    if events.send(Event::Connected).is_err() {
        tracing::warn!("Dropping Connected event: the event receiver is gone");
    }

    let e2e_runner = CspE2eProtocolRunner::new(CspE2eRunnerDeps {
        http_client,
        context: csp_e2e_context,
        d2m_outgoing: d2m_outgoing_tx,
        d2m_incoming: d2m_incoming_rx,
        csp_outgoing: csp_e2e_outgoing_tx.clone(),
        device_group_key,
        contacts,
        events,
        commands,
        user_identity: identity.minimal.user_identity,
        client_key: ClientKey::from(&identity.minimal.client_key),
        device_id: d2x_config.d2x_device_id.0,
        csp_nonces: nonces.scoped(NonceScope::CspE2e),
        d2x_nonces: nonces.scoped(NonceScope::D2x),
    });

    // None of these runners are `Send` (the in-memory providers use `Rc`), so they must run
    // concurrently within a single task via `select!` rather than via `tokio::spawn`. The E2E
    // runner also owns the command channel; it returns `Ok` on shutdown, ending the `select!`.
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
