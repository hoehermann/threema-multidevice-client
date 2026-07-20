//! Device to Mediator Protocol (D2M) connection: WebSocket handshake and payload flow.
//!
//! Adapted from libthreema's own `d2m_ping_pong` example, trimmed to what this CLI needs: relay
//! every post-handshake payload to/from the E2E driver, which uses it to run contact-creation
//! transactions and to reflect processed messages to sibling devices.
use anyhow::{anyhow, bail};
use futures_util::{SinkExt as _, TryStreamExt as _};
use libthreema::d2m::{D2mContext, D2mProtocol, D2mStateUpdate, payload::IncomingPayload, payload::OutgoingPayload};
use reqwest::StatusCode;
use tokio::{
    net::TcpStream,
    sync::mpsc,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::protocol::{CloseFrame, Message, frame::coding::CloseCode},
};
use tracing::{debug, error, trace};

pub struct D2mProtocolRunner {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    protocol: D2mProtocol,
}
impl D2mProtocolRunner {
    #[tracing::instrument(skip_all)]
    pub async fn new(context: D2mContext) -> anyhow::Result<Self> {
        let (d2m_protocol, url) = D2mProtocol::new(context);
        debug!(?url, "Establishing WebSocket connection to mediator server");
        let (stream, response) = connect_async(url).await?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            bail!(
                "Expected response to switch protocols ({expected}), got {actual}",
                expected = StatusCode::SWITCHING_PROTOCOLS,
                actual = response.status(),
            );
        }
        Ok(Self {
            stream,
            protocol: d2m_protocol,
        })
    }

    #[tracing::instrument(skip_all)]
    async fn run_handshake_flow(&mut self) -> anyhow::Result<()> {
        loop {
            let datagram = self.receive().await?;
            self.protocol.add_datagrams(vec![datagram])?;

            let Some(instruction) = self.protocol.poll()? else {
                continue;
            };

            if let Some(incoming_payload) = instruction.incoming_payload {
                let message = "Unexpected incoming payload during handshake";
                error!(?incoming_payload, message);
                bail!(message)
            }

            if let Some(datagram) = instruction.outgoing_datagram {
                self.send(datagram.0).await?;
            }

            if let Some(D2mStateUpdate::PostHandshake(server_info)) = instruction.state_update {
                tracing::info!(?server_info, "D2M handshake complete");
                break;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn run_payload_flow(
        &mut self,
        incoming: mpsc::Sender<IncomingPayload>,
        mut outgoing: mpsc::Receiver<OutgoingPayload>,
    ) -> anyhow::Result<()> {
        loop {
            let mut instruction = self.protocol.poll()?;
            if instruction.is_none() {
                instruction = tokio::select! {
                    datagram = self.receive() => {
                        self.protocol.add_datagrams(vec![datagram?])?;
                        None
                    },
                    Some(outgoing_payload) = outgoing.recv() => {
                        debug!(?outgoing_payload, "Sending D2M payload");
                        Some(self.protocol.create_payload(outgoing_payload)?)
                    }
                }
            }

            let Some(instruction) = instruction else {
                continue;
            };

            if let Some(state_update) = instruction.state_update {
                let message = "Unexpected state update after handshake";
                error!(?state_update, message);
                bail!(message)
            }

            if let Some(incoming_payload) = instruction.incoming_payload {
                debug!(?incoming_payload, "Received D2M payload");
                incoming.send(incoming_payload).await?;
            }

            if let Some(datagram) = instruction.outgoing_datagram {
                self.send(datagram.0).await?;
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn shutdown(mut self) -> anyhow::Result<()> {
        Ok(self
            .stream
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Bye".into(),
            }))
            .await?)
    }

    async fn send(&mut self, datagram: Vec<u8>) -> anyhow::Result<()> {
        trace!(length = datagram.len(), "Sending datagram");
        self.stream.send(Message::Binary(datagram.into())).await?;
        Ok(())
    }

    async fn receive(&mut self) -> anyhow::Result<Vec<u8>> {
        let datagram = loop {
            let message = self
                .stream
                .try_next()
                .await?
                .ok_or(anyhow!("WebSocket reading end closed"))?;
            match message {
                Message::Binary(bytes) => break bytes.to_vec(),
                Message::Text(text) => bail!("Received unexpected text message: {}", text.as_str()),
                Message::Ping(bytes) => {
                    self.stream.feed(Message::Pong(bytes)).await?;
                },
                Message::Pong(_) => {},
                Message::Close(close_frame) => {
                    tracing::info!(?close_frame, "Received close");
                },
                Message::Frame(_) => bail!("Received unexpected raw frame"),
            }
        };
        Ok(datagram)
    }

    /// Run the D2M connection to completion: handshake, then payload flow until stopped.
    pub async fn run(
        mut self,
        incoming: mpsc::Sender<IncomingPayload>,
        outgoing: mpsc::Receiver<OutgoingPayload>,
    ) -> anyhow::Result<()> {
        self.run_handshake_flow().await?;
        let result = self.run_payload_flow(incoming, outgoing).await;
        self.shutdown().await?;
        result
    }
}
