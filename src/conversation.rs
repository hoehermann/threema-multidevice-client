//! A [`ConversationProvider`] that prints incoming plain-text messages to stdout instead of
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

use crate::store::ContactStore;

/// Prints incoming messages to stdout. Delegates contact lookups to the shared contact store so
/// displayed names and existence checks stay consistent with the rest of the app.
pub struct PrintingConversationProvider {
    contacts: ContactStore,
    seen: HashSet<(ThreemaId, MessageId)>,
}

impl PrintingConversationProvider {
    pub fn new(contacts: ContactStore) -> Self {
        Self {
            contacts,
            seen: HashSet::new(),
        }
    }

    fn display_name(&self, identity: ThreemaId) -> String {
        let contact: Option<Contact> = self.contacts.get(identity).ok().flatten();
        let Some(contact) = contact else {
            return identity.to_string();
        };
        if let Some(nickname) = contact.nickname.filter(|name| !name.is_empty()) {
            return format!("{nickname} ({identity})");
        }
        let full_name = [contact.first_name, contact.last_name]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        if full_name.is_empty() {
            identity.to_string()
        } else {
            format!("{full_name} ({identity})")
        }
    }

    fn print_message(&self, sender_identity: ThreemaId, created_at: u64, body: &str, context: Option<String>) {
        let author = self.display_name(sender_identity);
        match context {
            Some(context) => println!("[{created_at}] {author} ({context}): {body}"),
            None => println!("[{created_at}] {author}: {body}"),
        }
    }
}

impl ConversationProvider for PrintingConversationProvider {
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
        let context = match &message.body {
            IncomingMessageBody::Contact(_) => {
                if !self.contacts.has(message.sender_identity)? {
                    return Err(ProviderError::InvalidState(
                        "Contact the incoming message refers to does not exist".to_owned(),
                    ));
                }
                None
            },
            IncomingMessageBody::Group(body) => Some(format!(
                "group {}/{}",
                body.group_identity.creator_identity, body.group_identity.group_id
            )),
        };

        if !self.seen.insert((message.sender_identity, message.id)) {
            tracing::warn!(
                sender_identity = ?message.sender_identity,
                message_id = ?message.id,
                "Discarding message that already exists"
            );
            return Ok(());
        }

        match &message.body {
            IncomingMessageBody::Contact(ContactMessageBody::Text(text)) => {
                self.print_message(message.sender_identity, message.created_at, &text.text, context);
            },
            IncomingMessageBody::Group(body) => match &body.body {
                GroupMessageBody::Text(text) => {
                    self.print_message(message.sender_identity, message.created_at, &text.text, context);
                },
                other => tracing::info!(?other, "Received non-text group message, not printing"),
            },
            other => tracing::info!(?other, "Received non-text message, not printing"),
        }

        Ok(())
    }
}
