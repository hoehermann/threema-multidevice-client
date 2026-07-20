//! Chat Server Protocol (CSP) connection: TCP handshake and payload flow.
//!
//! Adapted from libthreema's own `csp_e2e_receive` example, trimmed to what this CLI needs.
use std::io;

use anyhow::bail;
use libthreema::csp::{
    CspProtocol, CspProtocolContext, CspStateUpdate,
    payload::{IncomingPayload, MessageAck, MessageWithMetadataBox, OutgoingFrame, OutgoingPayload},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    sync::mpsc,
};
use tracing::{debug, error, trace, warn};

/// Incoming payloads forwarded to the E2E driver.
pub enum IncomingPayloadForCspE2e {
    Message(MessageWithMetadataBox),
    MessageAck(MessageAck),
}

/// Outgoing payloads accepted from the E2E driver.
pub enum OutgoingPayloadForCspE2e {
    MessageAck(MessageAck),
}
impl From<OutgoingPayloadForCspE2e> for OutgoingPayload {
    fn from(payload: OutgoingPayloadForCspE2e) -> Self {
        match payload {
            OutgoingPayloadForCspE2e::MessageAck(message_ack) => OutgoingPayload::MessageAck(message_ack),
        }
    }
}

pub struct PayloadQueuesForCspE2e {
    pub incoming: mpsc::Receiver<IncomingPayloadForCspE2e>,
    pub outgoing: mpsc::Sender<OutgoingPayloadForCspE2e>,
}

struct PayloadQueuesForCsp {
    incoming: mpsc::Sender<IncomingPayloadForCspE2e>,
    outgoing: mpsc::Receiver<OutgoingPayloadForCspE2e>,
}

pub struct CspProtocolRunner {
    stream: TcpStream,
    protocol: CspProtocol,
}
impl CspProtocolRunner {
    #[tracing::instrument(skip_all)]
    pub async fn new(
        server_address: Vec<(String, u16)>,
        context: CspProtocolContext,
    ) -> anyhow::Result<(Self, OutgoingFrame)> {
        debug!(?server_address, "Establishing TCP connection to chat server");
        let tcp_stream = TcpStream::connect(
            server_address
                .first()
                .expect("CSP config should have at least one address"),
        )
        .await?;
        let (csp_protocol, client_hello) = CspProtocol::new(context);
        Ok((
            Self {
                stream: tcp_stream,
                protocol: csp_protocol,
            },
            client_hello,
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn run_handshake_flow(&mut self, client_hello: OutgoingFrame) -> anyhow::Result<()> {
        self.send(&client_hello.0).await?;
        loop {
            let bytes = self.receive_required().await?;
            self.protocol.add_chunks(&[&bytes])?;

            let Some(instruction) = self.protocol.poll()? else {
                continue;
            };

            if let Some(incoming_payload) = instruction.incoming_payload {
                let message = "Unexpected incoming payload during handshake";
                error!(?incoming_payload, message);
                bail!(message)
            }

            if let Some(frame) = instruction.outgoing_frame {
                self.send(&frame.0).await?;
            }

            if let Some(CspStateUpdate::PostHandshake(login_ack_data)) = instruction.state_update {
                tracing::info!(?login_ack_data, "CSP handshake complete");
                break;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn run_payload_flow(&mut self, mut queues: PayloadQueuesForCsp) -> anyhow::Result<()> {
        let mut read_buffer = [0_u8; 8192];
        let mut next_instruction = None;

        loop {
            if next_instruction.is_none() {
                next_instruction = self.protocol.poll()?;
            }

            if next_instruction.is_none() {
                next_instruction = tokio::select! {
                    _ = self.stream.readable() => {
                        let length = self.try_receive(&mut read_buffer)?;
                        self.protocol
                            .add_chunks(&[read_buffer.get(..length)
                            .expect("Amount of read bytes should be available")])?;
                        None
                    },
                    outgoing_payload = queues.outgoing.recv() => {
                        if let Some(outgoing_payload) = outgoing_payload {
                            let outgoing_payload = OutgoingPayload::from(outgoing_payload);
                            debug!(?outgoing_payload, "Sending CSP payload");
                            Some(self.protocol.create_payload(&outgoing_payload)?)
                        } else {
                            break
                        }
                    }
                };
            }

            let Some(current_instruction) = next_instruction.take() else {
                continue;
            };

            if let Some(state_update) = current_instruction.state_update {
                let message = "Unexpected state update after handshake";
                error!(?state_update, message);
                bail!(message)
            }

            if let Some(incoming_payload) = current_instruction.incoming_payload {
                debug!(?incoming_payload, "Received CSP payload");
                match incoming_payload {
                    IncomingPayload::EchoRequest(echo_payload) => {
                        next_instruction = Some(
                            self.protocol
                                .create_payload(&OutgoingPayload::EchoResponse(echo_payload))?,
                        );
                    },
                    IncomingPayload::MessageWithMetadataBox(payload) => {
                        queues
                            .incoming
                            .send(IncomingPayloadForCspE2e::Message(payload))
                            .await?;
                    },
                    IncomingPayload::MessageAck(payload) => {
                        queues
                            .incoming
                            .send(IncomingPayloadForCspE2e::MessageAck(payload))
                            .await?;
                    },
                    IncomingPayload::EchoResponse(_)
                    | IncomingPayload::QueueSendComplete
                    | IncomingPayload::DeviceCookieChangeIndication
                    | IncomingPayload::CloseError(_)
                    | IncomingPayload::ServerAlert(_)
                    | IncomingPayload::UnknownPayload { .. } => {},
                }
            }

            if let Some(frame) = current_instruction.outgoing_frame {
                self.send(&frame.0).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        Ok(self.stream.shutdown().await?)
    }

    async fn send(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        trace!(length = bytes.len(), "Sending bytes");
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    async fn receive_required(&mut self) -> anyhow::Result<Vec<u8>> {
        let length = self.protocol.next_required_length()?;
        let mut buffer = vec![0; length];
        if length == 0 {
            return Ok(buffer);
        }
        let _ = self.stream.read_exact(&mut buffer).await?;
        match self.stream.try_read_buf(&mut buffer) {
            Ok(0) => bail!("TCP reading end closed"),
            Ok(length) => trace!(length, "Got additional bytes"),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => trace!("No additional bytes available"),
            Err(error) => return Err(error.into()),
        }
        Ok(buffer)
    }

    fn try_receive(&mut self, buffer: &mut [u8]) -> anyhow::Result<usize> {
        match self.stream.try_read(buffer) {
            Ok(0) => {
                warn!("TCP reading end closed");
                Ok(0)
            },
            Ok(length) => Ok(length),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    /// Run the CSP connection to completion: handshake, then payload flow until stopped.
    pub async fn run(
        mut self,
        client_hello: OutgoingFrame,
        incoming: mpsc::Sender<IncomingPayloadForCspE2e>,
        outgoing: mpsc::Receiver<OutgoingPayloadForCspE2e>,
    ) -> anyhow::Result<()> {
        self.run_handshake_flow(client_hello).await?;
        let result = self.run_payload_flow(PayloadQueuesForCsp { incoming, outgoing }).await;
        self.shutdown().await?;
        result
    }
}
