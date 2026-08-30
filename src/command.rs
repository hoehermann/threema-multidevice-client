//! Commands the embedder sends into the client.

/// Where to deliver an outgoing message.
#[derive(Debug, Clone)]
pub enum Recipient {
    /// One-to-one conversation with a contact.
    Contact {
        /// The contact's Threema ID.
        identity: String,
    },
    /// Group conversation.
    Group {
        /// Threema ID of the group's creator.
        creator_identity: String,
        group_id: u64,
    },
}

/// Everything the embedder can ask the client to do.
#[derive(Debug, Clone)]
pub enum Command {
    /// Send a plain-text message. Not implemented yet -- currently only logged, so the command
    /// topology is settled before the send flow exists.
    SendText { to: Recipient, text: String },
    /// Terminate the client: [`crate::run`] returns cleanly. Dropping the command sender has the
    /// same effect.
    Shutdown,
}
