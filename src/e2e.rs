//! Chat Server E2EE Protocol driver.
//!
//! Adapted from libthreema's own `csp_e2e_receive` example, but with the transaction/reflect
//! instructions actually wired up to a live D2M connection instead of `todo!()`, since this
//! client operates as a linked (multi-device) device.
use std::collections::HashMap;

use anyhow::{Context as _, anyhow, bail};
use libthreema::{
    common::{Delta, MessageId, Nonce, ThreemaId, keys::{ClientKey, DeviceGroupKey}, task::TaskLoop},
    csp_e2e::{
        CspE2eProtocol, CspE2eProtocolContextInit, D2mRole, ReflectId,
        contacts::{
            create::{
                CreateContactsInstruction, CreateContactsLoop, CreateContactsResponse, CreateContactsTask,
            },
            lookup::{
                CacheLookupPolicy, ContactResult, ContactsLookupInstruction, ContactsLookupResponse,
                ContactsLookupSubtask,
            },
            update::{UpdateContactsInstruction, UpdateContactsResponse},
        },
        message::task::{
            incoming::{
                IncomingMessageInstruction, IncomingMessageLoop, IncomingMessageResponse,
                IncomingMessageTask,
            },
            outgoing::encode_and_encrypt_message,
        },
        reflect::{ReflectFlags, ReflectInstruction, ReflectPayload, ReflectResponse},
        transaction::{
            begin::{BeginTransactionInstruction, BeginTransactionReply, BeginTransactionResponse},
            commit::{CommitTransactionInstruction, CommitTransactionResponse},
        },
    },
    d2m::payload::{
        BeginTransaction as D2mBeginTransaction, IncomingPayload as D2mIncomingPayload,
        OutgoingPayload as D2mOutgoingPayload, Reflect, ReflectFlags as D2mReflectFlags, ReflectedAck,
    },
    model::{
        contact::{Contact, ContactInit},
        message::{
            ContactMessageBody, MessageOverrides, OutgoingContactMessageBody, OutgoingMessage,
            OutgoingMessageBody, TextMessage as OutgoingTextMessage,
        },
        provider::{ContactProvider as _, NonceStorage as _},
    },
    protobuf::{
        self,
        common::CspE2eMessageType,
        d2d::envelope::Content,
    },
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    command::{Command, Recipient},
    conversation::contact_name,
    csp::{IncomingPayloadForCspE2e, OutgoingPayloadForCspE2e, PayloadQueuesForCspE2e},
    event::{Conversation, Event, TextMessage},
    store::{ContactStore, ScopedNonceStore},
};

/// Everything [`CspE2eProtocolRunner`] needs, bundled up so construction in `run()` stays
/// readable.
pub struct CspE2eRunnerDeps {
    pub http_client: reqwest::Client,
    pub context: CspE2eProtocolContextInit,
    pub d2m_outgoing: mpsc::Sender<D2mOutgoingPayload>,
    pub d2m_incoming: mpsc::Receiver<D2mIncomingPayload>,
    pub csp_outgoing: mpsc::Sender<OutgoingPayloadForCspE2e>,
    pub device_group_key: DeviceGroupKey,
    pub contacts: ContactStore,
    pub events: mpsc::UnboundedSender<Event>,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub user_identity: ThreemaId,
    pub client_key: ClientKey,
    /// This device's D2X device ID, stamped into reflected `d2d.Envelope`s.
    pub device_id: u64,
    pub csp_nonces: ScopedNonceStore,
    pub d2x_nonces: ScopedNonceStore,
}

/// Outcome of resolving a send's recipient.
#[expect(
    clippy::large_enum_variant,
    reason = "one short-lived value per send, destructured immediately; boxing would only add an allocation"
)]
enum ResolvedContact {
    Found(Contact),
    /// The recipient can't be messaged, with a reason for the user.
    Rejected(String),
}

/// An outgoing message that was handed to the chat server but not yet acknowledged by it.
struct PendingSend {
    /// The receiver's Threema ID (as a string, for the `ConversationId` of the `Sent` update and
    /// the success event).
    identity: String,
    text: String,
    created_at: u64,
}

pub struct CspE2eProtocolRunner {
    protocol: CspE2eProtocol,
    http_client: reqwest::Client,
    d2m_outgoing: mpsc::Sender<D2mOutgoingPayload>,
    d2m_incoming: mpsc::Receiver<D2mIncomingPayload>,
    /// For CSP payloads triggered by D2M events (leader promotion), where the per-call `queues`
    /// of `run` aren't in reach.
    csp_outgoing: mpsc::Sender<OutgoingPayloadForCspE2e>,
    /// Needed to decrypt `Reflected` envelopes (see `crate::d2d`) and encrypt our own.
    device_group_key: DeviceGroupKey,
    contacts: ContactStore,
    events: mpsc::UnboundedSender<Event>,
    commands: mpsc::UnboundedReceiver<Command>,
    user_identity: ThreemaId,
    client_key: ClientKey,
    device_id: u64,
    csp_nonces: ScopedNonceStore,
    d2x_nonces: ScopedNonceStore,
    /// Reflect IDs for our own outgoing-message reflections. Starts at `0x8000_0000` so it can
    /// never collide with libthreema's internal reflect counter (starts at 0) on the same D2M
    /// connection; IDs only need to be unique among unacknowledged reflects.
    next_reflect_id: u32,
    /// Sends awaiting the chat server's message-ack, keyed by (receiver, message ID).
    pending_sends: HashMap<(ThreemaId, u64), PendingSend>,
}

impl CspE2eProtocolRunner {
    pub fn new(deps: CspE2eRunnerDeps) -> Self {
        Self {
            protocol: CspE2eProtocol::new(deps.context),
            http_client: deps.http_client,
            d2m_outgoing: deps.d2m_outgoing,
            d2m_incoming: deps.d2m_incoming,
            csp_outgoing: deps.csp_outgoing,
            device_group_key: deps.device_group_key,
            contacts: deps.contacts,
            events: deps.events,
            commands: deps.commands,
            user_identity: deps.user_identity,
            client_key: deps.client_key,
            device_id: deps.device_id,
            csp_nonces: deps.csp_nonces,
            d2x_nonces: deps.d2x_nonces,
            next_reflect_id: 0x8000_0000,
            pending_sends: HashMap::new(),
        }
    }

    /// Run the message flow (receiving and sending) until stopped by [`Command::Shutdown`], a
    /// dropped command sender, or a connection error.
    #[tracing::instrument(skip_all)]
    pub async fn run(mut self, mut queues: PayloadQueuesForCspE2e) -> anyhow::Result<()> {
        let mut pending_task: Option<IncomingMessageTask> = None;

        loop {
            let Some(task) = &mut pending_task else {
                tokio::select! {
                    incoming = queues.incoming.recv() => {
                        match incoming {
                            Some(IncomingPayloadForCspE2e::Message(message)) => {
                                info!(?message, "Incoming message");
                                pending_task = Some(self.protocol.handle_incoming_message(message));
                            },
                            Some(IncomingPayloadForCspE2e::MessageAck(message_ack)) => {
                                self.handle_message_ack(message_ack).await?;
                            },
                            None => bail!("CSP incoming channel closed"),
                        }
                    },
                    d2m_incoming = self.d2m_incoming.recv() => {
                        let payload = d2m_incoming.ok_or_else(|| anyhow!("D2M incoming channel closed"))?;
                        self.handle_unsolicited_d2m(payload).await?;
                    },
                    // Only polled between incoming tasks: a send command arriving while an
                    // incoming message is being processed simply waits in the channel.
                    command = self.commands.recv() => match command {
                        Some(Command::SendText { to, text }) => {
                            self.handle_send_text(to, text, &mut queues).await?;
                        },
                        // A dropped sender is an implicit shutdown request.
                        Some(Command::Shutdown) | None => {
                            info!("Shutting down on command");
                            return Ok(());
                        },
                    },
                }
                continue;
            };

            match task.poll(self.protocol.context())? {
                IncomingMessageLoop::Instruction(IncomingMessageInstruction::FetchSender(instruction)) => {
                    let response = self.run_contacts_lookup_requests(instruction).await;
                    task.response(IncomingMessageResponse::FetchSender(response))?;
                },
                IncomingMessageLoop::Instruction(IncomingMessageInstruction::CreateContact(instruction)) => {
                    match instruction {
                        CreateContactsInstruction::BeginTransaction(instruction) => {
                            if let Some(response) = self.begin_transaction(instruction).await? {
                                task.response(IncomingMessageResponse::CreateContact(
                                    CreateContactsResponse::BeginTransactionResponse(response),
                                ))?;
                            }
                        },
                        CreateContactsInstruction::ReflectAndCommitTransaction(instruction) => {
                            task.response(IncomingMessageResponse::CreateContact(
                                CreateContactsResponse::CommitTransactionResponse(
                                    self.reflect_and_commit_transaction(instruction).await?,
                                ),
                            ))?;
                        },
                    }
                },
                IncomingMessageLoop::Instruction(IncomingMessageInstruction::UpdateContact(instruction)) => {
                    match instruction {
                        UpdateContactsInstruction::BeginTransaction(instruction) => {
                            if let Some(response) = self.begin_transaction(instruction).await? {
                                task.response(IncomingMessageResponse::UpdateContact(
                                    UpdateContactsResponse::BeginTransactionResponse(response),
                                ))?;
                            }
                        },
                        UpdateContactsInstruction::ReflectAndCommitTransaction(instruction) => {
                            task.response(IncomingMessageResponse::UpdateContact(
                                UpdateContactsResponse::CommitTransactionResponse(
                                    self.reflect_and_commit_transaction(instruction).await?,
                                ),
                            ))?;
                        },
                    }
                },
                IncomingMessageLoop::Instruction(IncomingMessageInstruction::ReflectMessage(instruction)) => {
                    task.response(IncomingMessageResponse::ReflectMessage(
                        self.reflect(instruction).await?,
                    ))?;
                },
                IncomingMessageLoop::Done(result) => {
                    if let Some(outgoing_message_ack) = result.outgoing_message_ack {
                        queues
                            .outgoing
                            .send(OutgoingPayloadForCspE2e::MessageAck(outgoing_message_ack))
                            .await?;
                    }
                    pending_task = None;
                },
            }
        }
    }

    /// Handle a D2M payload that arrived while we weren't specifically awaiting it.
    async fn handle_unsolicited_d2m(&mut self, payload: D2mIncomingPayload) -> anyhow::Result<()> {
        match payload {
            D2mIncomingPayload::RolePromotedToLeader => {
                info!("Promoted to D2M leader role");
                self.protocol.update_d2m_state(D2mRole::Leader)?;
                // The chat server withholds incoming messages from every multi-device-capable
                // connection (one that announced a `csp-device-id`) until it sends
                // `unblock-incoming-messages`, which only the D2M leader may do. Skipping this
                // stalls the whole identity's incoming queue: sibling devices are non-leaders
                // (blocked by the server) and would never receive contact replies or delivery
                // receipts until this client disconnects and leadership moves on. Duplicate
                // sends after a D2M reconnect are harmless.
                self.csp_outgoing
                    .send(OutgoingPayloadForCspE2e::UnblockIncomingMessages)
                    .await?;
            },
            D2mIncomingPayload::Reflected(reflected) => {
                // Messages sent/received while a sibling device (e.g. the phone) held the D2M
                // leader role arrive here rather than via CSP -- decode and print if it's a
                // plain-text message (see `crate::d2d` for why libthreema itself doesn't decode
                // this). A decode failure shouldn't be fatal -- still ack it below regardless, so
                // the mediator's reflection queue keeps moving.
                if let Err(error) =
                    crate::d2d::handle_reflected_envelope(&self.device_group_key, &reflected.envelope, &self.contacts, &self.events)
                {
                    error!(reflect_id = reflected.reflect_id, ?error, "Failed to handle reflected D2D envelope");
                }
                self.d2m_outgoing
                    .send(D2mOutgoingPayload::ReflectedAck(ReflectedAck {
                        reflect_id: reflected.reflect_id,
                    }))
                    .await?;
            },
            D2mIncomingPayload::ReflectionQueueDry => debug!("D2M reflection queue is dry"),
            other => debug!(?other, "Unhandled D2M payload"),
        }
        Ok(())
    }

    /// Runs the HTTPS requests a contact lookup asked for. Shared by the incoming-message path
    /// (libthreema looking up an unknown sender) and by our own lookups for outgoing messages.
    async fn run_contacts_lookup_requests(
        &self,
        instruction: ContactsLookupInstruction,
    ) -> ContactsLookupResponse {
        let work_directory_request_future = async {
            match instruction.work_directory_request {
                Some(work_directory_request) => work_directory_request.send(&self.http_client).await.map(Some),
                None => Ok(None),
            }
        };
        let (directory_result, work_directory_result) = tokio::join!(
            instruction.directory_request.send(&self.http_client),
            work_directory_request_future,
        );
        ContactsLookupResponse {
            directory_result,
            work_directory_result: work_directory_result.transpose(),
        }
    }

    /// Looks an identity up at the directory server, returning what the directory knows about it.
    async fn lookup_contact(&mut self, identity: ThreemaId) -> anyhow::Result<Option<ContactResult>> {
        let mut lookup = ContactsLookupSubtask::new(vec![identity], CacheLookupPolicy::Allow);
        loop {
            match lookup.poll(self.protocol.context())? {
                TaskLoop::Instruction(instruction) => {
                    let response = self.run_contacts_lookup_requests(instruction).await;
                    lookup.response(response)?;
                },
                TaskLoop::Done(mut contacts) => return Ok(contacts.remove(&identity)),
            }
        }
    }

    /// Stores a freshly looked-up contact and syncs it to the other devices, via the same
    /// transaction machinery libthreema uses when the receive path meets an unknown sender.
    async fn create_contact(&mut self, contact: ContactInit) -> anyhow::Result<()> {
        let mut task = CreateContactsTask::new(vec![contact]);
        loop {
            match task.poll(self.protocol.context())? {
                CreateContactsLoop::Instruction(CreateContactsInstruction::BeginTransaction(instruction)) => {
                    if let Some(response) = self.begin_transaction(instruction).await? {
                        task.response(CreateContactsResponse::BeginTransactionResponse(response))?;
                    }
                },
                CreateContactsLoop::Instruction(CreateContactsInstruction::ReflectAndCommitTransaction(
                    instruction,
                )) => {
                    task.response(CreateContactsResponse::CommitTransactionResponse(
                        self.reflect_and_commit_transaction(instruction).await?,
                    ))?;
                },
                CreateContactsLoop::Done(_) => return Ok(()),
            }
        }
    }

    /// Resolves a recipient the contact store doesn't know yet: look it up at the directory and,
    /// if it exists, store and sync it.
    async fn resolve_unknown_contact(&mut self, identity: ThreemaId) -> anyhow::Result<ResolvedContact> {
        info!(%identity, "Contact is unknown, looking it up at the directory");
        // A lookup failure (network trouble, directory hiccup) is specific to this send attempt
        // and must not take the whole connection down.
        let result = match self.lookup_contact(identity).await {
            Ok(result) => result,
            Err(error) => {
                return Ok(ResolvedContact::Rejected(format!(
                    "Looking {identity} up failed: {error:#}"
                )));
            },
        };

        match result {
            Some(ContactResult::ExistingContact(contact)) => Ok(ResolvedContact::Found(contact)),
            Some(ContactResult::NewContact(contact)) => {
                // Creating it is what persists the public key and tells the other devices about
                // the new contact; a failure here is a D2M problem, so it stays fatal.
                self.create_contact(contact.clone()).await?;
                info!(%identity, "Stored contact looked up at the directory");
                Ok(ResolvedContact::Found(contact))
            },
            Some(ContactResult::User) => {
                Ok(ResolvedContact::Rejected("Cannot send a message to yourself".to_owned()))
            },
            Some(ContactResult::Invalid(identity)) => Ok(ResolvedContact::Rejected(format!(
                "{identity} does not exist or has been revoked"
            ))),
            None => Ok(ResolvedContact::Rejected(format!(
                "The directory returned no result for {identity}"
            ))),
        }
    }

    /// Report a failed send attempt to the embedder. Never fatal for the connection.
    fn send_failed(&self, conversation: Conversation, reason: impl Into<String>) {
        let reason = reason.into();
        warn!(?conversation, reason, "Send failed");
        if self.events.send(Event::SendFailed { conversation, reason }).is_err() {
            warn!("Dropping send-failed event: the event receiver is gone");
        }
    }

    /// Send a plain-text message to a contact: reflect it to sibling devices (awaiting the
    /// mediator's acknowledgement), then hand it to the chat server. The server's message-ack is
    /// correlated later in [`Self::handle_message_ack`].
    ///
    /// Recipient-related problems (unknown contact, unsupported conversation type) are reported
    /// via [`Event::SendFailed`] and return `Ok(())`; only connection-level failures are `Err`.
    async fn handle_send_text(
        &mut self,
        to: Recipient,
        text: String,
        queues: &mut PayloadQueuesForCspE2e,
    ) -> anyhow::Result<()> {
        let identity = match to {
            Recipient::Contact { identity } => identity,
            Recipient::Group { creator_identity, group_id } => {
                self.send_failed(
                    Conversation::Group { creator_identity, group_id },
                    "Group messages are not supported yet",
                );
                return Ok(());
            },
        };
        // TODO: should not be necessary, Pidgin has not been observed to mangle the who field
        let identity = identity.to_uppercase();
        let conversation = |name: Option<String>| Conversation::Contact { identity: identity.clone(), name };

        let Ok(receiver) = identity.parse::<ThreemaId>() else {
            self.send_failed(conversation(None), format!("'{identity}' is not a valid Threema ID"));
            return Ok(());
        };
        let contact = match self.contacts.get(receiver) {
            Ok(Some(contact)) => contact,
            // Not known yet: ask the directory for its public key. Contacts otherwise only reach
            // this client via a sibling's contact sync, which never happens for contacts that
            // already existed when this device was linked.
            Ok(None) => match self.resolve_unknown_contact(receiver).await? {
                ResolvedContact::Found(contact) => contact,
                ResolvedContact::Rejected(reason) => {
                    self.send_failed(conversation(None), reason);
                    return Ok(());
                },
            },
            Err(error) => {
                self.send_failed(conversation(None), format!("Contact store lookup failed: {error}"));
                return Ok(());
            },
        };

        let message = OutgoingMessage {
            id: MessageId::random(),
            overrides: MessageOverrides::default(),
            created_at: now_unix_ms(),
            body: OutgoingMessageBody::Contact(OutgoingContactMessageBody {
                receiver_identity: receiver,
                body: ContactMessageBody::Text(OutgoingTextMessage { text: text.clone() }),
            }),
        };

        // Encrypt the CSP payload and record its nonce before anything leaves this device, so
        // replay protection holds even if we crash mid-send.
        let nonce = Nonce::random();
        let shared_secret = self.client_key.derive_csp_e2e_key(&contact.public_key);
        let outgoing_box = encode_and_encrypt_message(
            self.user_identity,
            (None, Delta::Unchanged),
            receiver,
            shared_secret,
            &message,
            nonce.clone(),
        );
        self.csp_nonces
            .add_many(vec![nonce.clone()])
            .map_err(|error| anyhow!("Failed to record CSP nonce: {error}"))?;

        // Reflect the outgoing message to sibling devices first (and wait for the mediator's
        // acknowledgement), as the multi-device protocol requires -- only then hand it to the
        // chat server.
        self.reflect_content(outgoing_text_content(&identity, &message.id, message.created_at, &text, &nonce))
            .await?;
        let created_at = message.created_at;
        let message_id = message.id;
        queues
            .outgoing
            .send(OutgoingPayloadForCspE2e::Message(
                outgoing_box
                    .try_into()
                    .map_err(|error| anyhow!("Encoding outgoing message failed: {error}"))?,
            ))
            .await?;

        self.pending_sends
            .insert((receiver, message_id.0), PendingSend { identity, text, created_at });
        Ok(())
    }

    /// Handle the chat server's acknowledgement of an outgoing message: reflect the `Sent` update
    /// to sibling devices and emit the message as an outgoing [`Event::TextMessage`] (the
    /// embedder's cue to display it as sent).
    ///
    /// Note: for outgoing messages, the ack's `sender_identity` field holds the *receiver*.
    async fn handle_message_ack(
        &mut self,
        ack: libthreema::csp::payload::MessageAck,
    ) -> anyhow::Result<()> {
        let Some(pending) = self.pending_sends.remove(&(ack.sender_identity, ack.id.0)) else {
            warn!(?ack, "Message-ack without a matching pending send");
            return Ok(());
        };
        info!(?ack, "Outgoing message acknowledged by the server");

        self.reflect_content(Content::OutgoingMessageUpdate(protobuf::d2d::OutgoingMessageUpdate {
            updates: vec![protobuf::d2d::outgoing_message_update::Update {
                conversation: Some(contact_conversation_id(&pending.identity)),
                message_id: ack.id.0,
                update: Some(protobuf::d2d::outgoing_message_update::update::Update::Sent(
                    protobuf::d2d::outgoing_message_update::Sent {},
                )),
            }],
        }))
        .await?;

        let name = contact_name(&self.contacts, ack.sender_identity);
        if self
            .events
            .send(Event::TextMessage(TextMessage {
                timestamp_ms: pending.created_at,
                outgoing: true,
                conversation: Conversation::Contact { identity: pending.identity, name },
                sender_identity: None,
                sender_name: None,
                text: pending.text,
            }))
            .is_err()
        {
            warn!("Dropping sent-message event: the event receiver is gone");
        }
        Ok(())
    }

    /// Encrypt `content` into a `d2d.Envelope`, reflect it to the mediator, and wait for the
    /// acknowledgement. The envelope's nonce is recorded in the D2X nonce store.
    async fn reflect_content(&mut self, content: Content) -> anyhow::Result<()> {
        let envelope = build_envelope(self.device_id, content);
        let (encrypted, reflect_nonce) = self
            .device_group_key
            .encrypt_reflected_envelope(&envelope)
            .map_err(|_| anyhow!("Encrypting reflect envelope failed"))?;
        self.d2x_nonces
            .add_many(vec![reflect_nonce])
            .map_err(|error| anyhow!("Failed to record D2X nonce: {error}"))?;

        self.next_reflect_id = self.next_reflect_id.wrapping_add(1);
        let payload = ReflectPayload {
            flags: ReflectFlags::default(),
            id: ReflectId(self.next_reflect_id),
            envelope: encrypted,
        };
        let _ = self
            .reflect(ReflectInstruction {
                reflect_messages: vec![payload],
            })
            .await
            .context("Reflecting to the mediator failed")?;
        Ok(())
    }

    async fn send_reflect(&mut self, payload: ReflectPayload) -> anyhow::Result<()> {
        let flags = if payload.flags.ephemeral {
            D2mReflectFlags(D2mReflectFlags::EPHEMERAL_MARKER)
        } else {
            D2mReflectFlags(0)
        };
        self.d2m_outgoing
            .send(D2mOutgoingPayload::Reflect(Reflect {
                flags,
                reflect_id: payload.id.0,
                envelope: payload.envelope,
            }))
            .await?;
        Ok(())
    }

    fn reflect_ack_id(payload: &D2mIncomingPayload) -> Option<ReflectId> {
        match payload {
            D2mIncomingPayload::ReflectAck(ack) => Some(ReflectId(ack.reflect_id)),
            _ => None,
        }
    }

    async fn next_d2m_incoming(&mut self) -> anyhow::Result<D2mIncomingPayload> {
        self.d2m_incoming
            .recv()
            .await
            .ok_or_else(|| anyhow!("D2M incoming channel closed"))
    }

    async fn begin_transaction(
        &mut self,
        instruction: BeginTransactionInstruction,
    ) -> anyhow::Result<Option<BeginTransactionResponse>> {
        match instruction {
            BeginTransactionInstruction::TransactionRejected => loop {
                let payload = self.next_d2m_incoming().await?;
                if matches!(payload, D2mIncomingPayload::TransactionEnded(_)) {
                    return Ok(None);
                }
                self.handle_unsolicited_d2m(payload).await?;
            },
            BeginTransactionInstruction::BeginTransaction { message } => {
                self.d2m_outgoing
                    .send(D2mOutgoingPayload::BeginTransaction(D2mBeginTransaction {
                        encrypted_scope: message.encrypted_scope,
                        ttl: (message.ttl != 0).then(|| std::time::Duration::from_secs(u64::from(message.ttl))),
                    }))
                    .await?;
                loop {
                    match self.next_d2m_incoming().await? {
                        D2mIncomingPayload::BeginTransactionAck => {
                            return Ok(Some(BeginTransactionResponse::BeginTransactionReply(
                                BeginTransactionReply::BeginTransactionAck(Default::default()),
                            )));
                        },
                        D2mIncomingPayload::TransactionRejected(rejected) => {
                            return Ok(Some(BeginTransactionResponse::BeginTransactionReply(
                                BeginTransactionReply::TransactionRejected(protobuf::d2m::TransactionRejected {
                                    device_id: rejected.device_id.0,
                                    encrypted_scope: rejected.encrypted_scope,
                                }),
                            )));
                        },
                        other => self.handle_unsolicited_d2m(other).await?,
                    }
                }
            },
            BeginTransactionInstruction::AbortTransaction { message: _ } => {
                self.d2m_outgoing.send(D2mOutgoingPayload::CommitTransaction).await?;
                loop {
                    match self.next_d2m_incoming().await? {
                        D2mIncomingPayload::CommitTransactionAck => {
                            return Ok(Some(BeginTransactionResponse::AbortTransactionResponse(
                                Default::default(),
                            )));
                        },
                        other => self.handle_unsolicited_d2m(other).await?,
                    }
                }
            },
        }
    }

    async fn reflect_and_commit_transaction(
        &mut self,
        instruction: CommitTransactionInstruction,
    ) -> anyhow::Result<CommitTransactionResponse> {
        for payload in instruction.reflect_messages {
            self.send_reflect(payload).await?;
        }
        self.d2m_outgoing.send(D2mOutgoingPayload::CommitTransaction).await?;

        let mut acknowledged_reflect_ids = Vec::new();
        loop {
            let payload = self.next_d2m_incoming().await?;
            if matches!(payload, D2mIncomingPayload::CommitTransactionAck) {
                return Ok(CommitTransactionResponse {
                    acknowledged_reflect_ids,
                    commit_transaction_ack: Default::default(),
                });
            }
            if let Some(reflect_id) = Self::reflect_ack_id(&payload) {
                acknowledged_reflect_ids.push(reflect_id);
            } else {
                self.handle_unsolicited_d2m(payload).await?;
            }
        }
    }

    async fn reflect(&mut self, instruction: ReflectInstruction) -> anyhow::Result<ReflectResponse> {
        let mut pending = instruction
            .reflect_messages
            .iter()
            .filter(|message| !message.flags.ephemeral)
            .count();
        for payload in instruction.reflect_messages {
            self.send_reflect(payload).await?;
        }

        let mut acknowledged_reflect_ids = Vec::new();
        while pending > 0 {
            let payload = self.next_d2m_incoming().await?;
            if let Some(reflect_id) = Self::reflect_ack_id(&payload) {
                acknowledged_reflect_ids.push(reflect_id);
                pending -= 1;
            } else {
                self.handle_unsolicited_d2m(payload).await?;
            }
        }
        Ok(ReflectResponse { acknowledged_reflect_ids })
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after the epoch")
        .as_millis()
        .try_into()
        .expect("milliseconds since the epoch should fit a u64")
}

fn contact_conversation_id(identity: &str) -> protobuf::d2d::ConversationId {
    protobuf::d2d::ConversationId {
        id: Some(protobuf::d2d::conversation_id::Id::Contact(identity.to_owned())),
    }
}

/// Build the `d2d.OutgoingMessage` content for reflecting an outgoing plain-text message. The
/// body carries the *plaintext* text; `nonce` is the CSP message nonce, shared with sibling
/// devices for their nonce stores.
fn outgoing_text_content(
    receiver_identity: &str,
    message_id: &MessageId,
    created_at: u64,
    text: &str,
    nonce: &Nonce,
) -> Content {
    Content::OutgoingMessage(protobuf::d2d::OutgoingMessage {
        conversation: Some(contact_conversation_id(receiver_identity)),
        message_id: message_id.0,
        thread_message_id: None,
        created_at,
        r#type: CspE2eMessageType::Text as i32,
        body: text.as_bytes().to_vec(),
        nonces: vec![nonce.0.to_vec()],
    })
}

/// Wrap reflect content into this device's `d2d.Envelope` (whose padding is applied during
/// encryption -- see `DeviceGroupKey::encrypt_reflected_envelope`).
fn build_envelope(device_id: u64, content: Content) -> protobuf::d2d::Envelope {
    protobuf::d2d::Envelope {
        #[allow(deprecated, reason = "will be filled by encode_to_vec_padded during encryption")]
        padding: vec![],
        device_id,
        protocol_version: protobuf::d2d::ProtocolVersion::V03 as u32,
        content: Some(content),
    }
}

#[cfg(test)]
mod tests {
    use libthreema::common::keys::DeviceGroupKey;
    use tokio::sync::mpsc;

    use super::*;
    use crate::event::{Conversation, Event};
    use crate::store::ContactStore;

    /// The envelopes this client reflects for its own sends must round-trip through the same
    /// decode path (`crate::d2d`) that handles envelopes reflected *by* sibling devices --
    /// i.e. our sends look exactly like a phone's sends to the rest of the device group.
    #[test]
    fn reflected_outgoing_text_round_trips_through_d2d() {
        let device_group_key = DeviceGroupKey::from([7_u8; 32]);
        let message_id = MessageId(42);
        let nonce = Nonce([9_u8; 24]);
        let envelope = build_envelope(
            1,
            outgoing_text_content("ECHOECHO", &message_id, 1_700_000_000_000, "hello from this client", &nonce),
        );
        let (encrypted, _nonce) = device_group_key
            .encrypt_reflected_envelope(&envelope)
            .expect("encryption should succeed");

        let contacts_path = std::env::temp_dir().join(format!(
            "threema-cli-test-e2e-contacts-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after the epoch")
                .as_nanos()
        ));
        let contacts = ContactStore::load(contacts_path.clone(), "USERUSER".parse().expect("valid identity"))
            .expect("load empty contact store");
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        crate::d2d::handle_reflected_envelope(&device_group_key, &encrypted, &contacts, &events_tx)
            .expect("decrypting and decoding our own envelope should succeed");
        let _ = std::fs::remove_file(&contacts_path);

        let Event::TextMessage(message) = events_rx.try_recv().expect("an event should have been emitted")
        else {
            panic!("the emitted event should be a text message");
        };
        assert!(message.outgoing);
        assert_eq!(message.timestamp_ms, 1_700_000_000_000);
        assert_eq!(message.text, "hello from this client");
        assert!(
            matches!(message.conversation, Conversation::Contact { ref identity, .. } if identity == "ECHOECHO")
        );
    }
}
