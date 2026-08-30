//! Events the client library emits towards its embedder.
//!
//! All types are plain owned data (`Send`, no protocol or provider types), so events can cross
//! thread boundaries and be converted for foreign-language embedders easily.

/// The conversation a message belongs to.
#[derive(Debug, Clone)]
pub enum Conversation {
    /// One-to-one conversation with a contact.
    Contact {
        /// The contact's Threema ID.
        identity: String,
        /// The contact's name as known by the contact store, if any.
        name: Option<String>,
    },
    /// Group conversation.
    Group {
        /// Threema ID of the group's creator.
        creator_identity: String,
        group_id: u64,
    },
    /// Distribution list.
    DistributionList { id: u64 },
    /// The conversation could not be determined. Notably the case for reflected incoming group
    /// messages: their group identity is embedded in the (undecoded) message body.
    Unknown,
}

/// A plain-text message, either received from a contact or sent by the user themself (on another
/// linked device, reflected here).
#[derive(Debug, Clone)]
pub struct TextMessage {
    /// Milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// Whether the user themself sent this message (from any linked device).
    pub outgoing: bool,
    pub conversation: Conversation,
    /// The sender's Threema ID; `None` when `outgoing` (the sender is the user themself).
    pub sender_identity: Option<String>,
    /// The sender's name as known by the contact store, if any.
    pub sender_name: Option<String>,
    pub text: String,
}

/// Everything the client reports to its embedder.
#[derive(Debug, Clone)]
pub enum Event {
    /// A plain-text message arrived, either directly via the chat server or reflected from a
    /// sibling device via the mediator.
    TextMessage(TextMessage),
}
