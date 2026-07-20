//! Chat Server E2EE Protocol driver.
//!
//! Adapted from libthreema's own `csp_e2e_receive` example, but with the transaction/reflect
//! instructions actually wired up to a live D2M connection instead of `todo!()`, since this
//! client operates as a linked (multi-device) device.
use anyhow::{anyhow, bail};
use libthreema::{
    csp_e2e::{
        CspE2eProtocol, CspE2eProtocolContextInit, D2mRole, ReflectId,
        contacts::{
            create::{CreateContactsInstruction, CreateContactsResponse},
            lookup::ContactsLookupResponse,
            update::{UpdateContactsInstruction, UpdateContactsResponse},
        },
        message::task::incoming::{
            IncomingMessageInstruction, IncomingMessageLoop, IncomingMessageResponse, IncomingMessageTask,
        },
        reflect::{ReflectInstruction, ReflectPayload, ReflectResponse},
        transaction::{
            begin::{BeginTransactionInstruction, BeginTransactionReply, BeginTransactionResponse},
            commit::{CommitTransactionInstruction, CommitTransactionResponse},
        },
    },
    d2m::payload::{
        BeginTransaction as D2mBeginTransaction, IncomingPayload as D2mIncomingPayload,
        OutgoingPayload as D2mOutgoingPayload, Reflect, ReflectFlags as D2mReflectFlags, ReflectedAck,
    },
    protobuf,
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::csp::{IncomingPayloadForCspE2e, OutgoingPayloadForCspE2e, PayloadQueuesForCspE2e};

pub struct CspE2eProtocolRunner {
    protocol: CspE2eProtocol,
    http_client: reqwest::Client,
    d2m_outgoing: mpsc::Sender<D2mOutgoingPayload>,
    d2m_incoming: mpsc::Receiver<D2mIncomingPayload>,
}

impl CspE2eProtocolRunner {
    pub fn new(
        http_client: reqwest::Client,
        context: CspE2eProtocolContextInit,
        d2m_outgoing: mpsc::Sender<D2mOutgoingPayload>,
        d2m_incoming: mpsc::Receiver<D2mIncomingPayload>,
    ) -> Self {
        Self {
            protocol: CspE2eProtocol::new(context),
            http_client,
            d2m_outgoing,
            d2m_incoming,
        }
    }

    /// Run the receive flow until stopped.
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
                                warn!(?message_ack, "Unexpected message-ack");
                            },
                            None => bail!("CSP incoming channel closed"),
                        }
                    },
                    d2m_incoming = self.d2m_incoming.recv() => {
                        let payload = d2m_incoming.ok_or_else(|| anyhow!("D2M incoming channel closed"))?;
                        self.handle_unsolicited_d2m(payload).await?;
                    },
                }
                continue;
            };

            match task.poll(self.protocol.context())? {
                IncomingMessageLoop::Instruction(IncomingMessageInstruction::FetchSender(instruction)) => {
                    let work_directory_request_future = async {
                        match instruction.work_directory_request {
                            Some(work_directory_request) => {
                                work_directory_request.send(&self.http_client).await.map(Some)
                            },
                            None => Ok(None),
                        }
                    };
                    let (directory_result, work_directory_result) = tokio::join!(
                        instruction.directory_request.send(&self.http_client),
                        work_directory_request_future,
                    );
                    task.response(IncomingMessageResponse::FetchSender(ContactsLookupResponse {
                        directory_result,
                        work_directory_result: work_directory_result.transpose(),
                    }))?;
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
            },
            D2mIncomingPayload::Reflected(reflected) => {
                // Processing D2D-reflected sync data (contact/group/settings sync from sibling
                // devices) isn't implemented -- libthreema doesn't expose a decoder for it yet.
                // Acknowledge it anyway so the mediator's reflection queue keeps moving.
                debug!(reflect_id = reflected.reflect_id, "Ignoring reflected D2D envelope");
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
