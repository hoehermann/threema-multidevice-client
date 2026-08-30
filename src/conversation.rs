//! A [`ConversationProvider`] that emits incoming plain-text messages as [`Event`]s instead of
//! persisting them.
use std::collections::HashSet;

use libthreema::{
    common::{MessageId, ThreemaId},
    model::{
        contact::Contact,
        message::{ContactMessageBody, GroupMessageBody, IncomingMessage, IncomingMessageBody},
        provider::{ContactProvider, ConversationProvider, ProviderError},
    },
};
use tokio::sync::mpsc;

use crate::{
    event::{Conversation, Event, TextMessage},
    store::ContactStore,
};

/// Resolve a contact's name via the contact store: nickname, else first/last name, else `None`.
pub fn contact_name(contacts: &ContactStore, identity: ThreemaId) -> Option<String> {
    let contact: Contact = contacts.get(identity).ok().flatten()?;
    if let Some(nickname) = contact.nickname.filter(|name| !name.is_empty()) {
        return Some(nickname);
    }
    let full_name = [contact.first_name, contact.last_name]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    (!full_name.is_empty()).then_some(full_name)
}

/// Emits incoming messages as [`Event`]s. Delegates contact lookups to the shared contact store so
/// reported names and existence checks stay consistent with the rest of the app.
pub struct EventConversationProvider {
    contacts: ContactStore,
    seen: HashSet<(ThreemaId, MessageId)>,
    events: mpsc::UnboundedSender<Event>,
}

impl EventConversationProvider {
    pub fn new(contacts: ContactStore, events: mpsc::UnboundedSender<Event>) -> Self {
        Self {
            contacts,
            seen: HashSet::new(),
            events,
        }
    }

    fn emit(&self, message: TextMessage) {
        // The only send failure is a dropped receiver, i.e. the embedder is shutting down.
        if self.events.send(Event::TextMessage(message)).is_err() {
            tracing::warn!("Dropping message event: the event receiver is gone");
        }
    }
}

impl ConversationProvider for EventConversationProvider {
    fn message_is_marked_used(
        &self,
        sender_identity: ThreemaId,
        id: MessageId,
    ) -> Result<bool, ProviderError> {
        Ok(self.seen.contains(&(sender_identity, id)))
    }

    fn set_typing_indicator(
        &mut self,
        _sender_identity: ThreemaId,
        _is_typing: bool,
    ) -> Result<(), ProviderError> {
        // Not relevant for a headless client.
        Ok(())
    }

    fn add_or_update_incoming_message(&mut self, message: IncomingMessage) -> Result<(), ProviderError> {
        // Validate + determine conversation context, per the trait contract.
        let conversation = match &message.body {
            IncomingMessageBody::Contact(_) => {
                if !self.contacts.has(message.sender_identity)? {
                    return Err(ProviderError::InvalidState(
                        "Contact the incoming message refers to does not exist".to_owned(),
                    ));
                }
                Conversation::Contact {
                    identity: message.sender_identity.to_string(),
                    name: contact_name(&self.contacts, message.sender_identity),
                }
            },
            IncomingMessageBody::Group(body) => Conversation::Group {
                creator_identity: body.group_identity.creator_identity.to_string(),
                group_id: body.group_identity.group_id,
            },
        };

        if !self.seen.insert((message.sender_identity, message.id)) {
            tracing::warn!(
                sender_identity = ?message.sender_identity,
                message_id = ?message.id,
                "Discarding message that already exists"
            );
            return Ok(());
        }

        let text = match &message.body {
            IncomingMessageBody::Contact(ContactMessageBody::Text(text)) => &text.text,
            IncomingMessageBody::Group(body) => match &body.body {
                GroupMessageBody::Text(text) => &text.text,
                other => {
                    tracing::info!(?other, "Received non-text group message, not reporting");
                    return Ok(());
                },
            },
            other => {
                tracing::info!(?other, "Received non-text message, not reporting");
                return Ok(());
            },
        };

        self.emit(TextMessage {
            timestamp_ms: message.created_at,
            outgoing: false,
            conversation,
            sender_identity: Some(message.sender_identity.to_string()),
            sender_name: contact_name(&self.contacts, message.sender_identity),
            text: text.clone(),
        });

        Ok(())
    }
}
