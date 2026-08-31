//! Decodes D2D `Reflected` envelopes -- the sync messages a *sibling* device (e.g. the phone,
//! whenever it -- not this CLI -- holds the D2M "leader" role) sends about messages it has sent or
//! received, so every linked device's history stays consistent.
//!
//! libthreema's `CspE2eProtocol` has no decoder for this (only the encode side, used when *this*
//! device is leader and needs to reflect to others -- see `csp_e2e::reflect`). Decryption itself
//! goes through `DeviceGroupKey::decrypt_reflected_envelope`, added directly to the vendored
//! libthreema (it already has the correct, tested DGRK derivation crate-private; exposing one
//! narrow method beats reimplementing that derivation independently). What's left to do here is
//! just parsing the decrypted bytes as a `d2d.Envelope` and emitting an [`Event`] for the two
//! content variants that carry a plain-text message -- everything else (contact/group/settings
//! sync, etc.) is out of scope, same boundary as the CSP-E2E side only handling text messages.
use anyhow::Context as _;
use libthreema::{
    common::{FeatureMask, GroupIdentity, ThreemaId, keys::{DeviceGroupKey, PublicKey}},
    model::{contact::Contact, provider::ContactProvider as _},
    protobuf::{
        self,
        common::CspE2eMessageType,
        d2d::{Envelope, envelope::Content},
        d2d_sync::contact as protobuf_contact,
    },
};
use prost::Message as _;
use tokio::sync::mpsc;

use crate::{
    conversation::contact_name,
    event::{Conversation, Event, TextMessage},
    store::ContactStore,
};

fn conversation(contacts: &ContactStore, conversation: Option<protobuf::d2d::ConversationId>) -> Conversation {
    use protobuf::d2d::conversation_id::Id;
    match conversation.and_then(|conversation| conversation.id) {
        Some(Id::Contact(identity)) => {
            let name = identity
                .parse::<ThreemaId>()
                .ok()
                .and_then(|identity| contact_name(contacts, identity));
            Conversation::Contact { identity, name }
        },
        Some(Id::Group(group)) => match GroupIdentity::try_from(&group) {
            Ok(group) => Conversation::Group {
                creator_identity: group.creator_identity.to_string(),
                group_id: group.group_id,
            },
            Err(_) => Conversation::Unknown,
        },
        Some(Id::DistributionList(id)) => Conversation::DistributionList { id },
        None => Conversation::Unknown,
    }
}

fn is_text_type(message_type: i32) -> bool {
    matches!(
        CspE2eMessageType::try_from(message_type),
        Ok(CspE2eMessageType::Text | CspE2eMessageType::GroupText)
    )
}

/// Maps a contact as synced by a sibling device onto libthreema's contact model. libthreema only
/// has the encode direction (`ContactInit::encode`, used when *this* device reflects a contact it
/// created), so the decode side lives here. Absent optional fields fall back to the protobuf
/// defaults, mirroring how [`crate::store`] reads its own persisted contacts.
fn decode_synced_contact(contact: protobuf::d2d_sync::Contact) -> anyhow::Result<Contact> {
    let identity: ThreemaId = contact
        .identity
        .parse()
        .with_context(|| format!("Synced contact has an invalid Threema ID: {}", contact.identity))?;
    let public_key = contact
        .public_key
        .context("Synced contact carries no public key")?;
    let public_key = PublicKey::try_from(public_key.as_slice())
        .map_err(|_| anyhow::anyhow!("Synced contact has a malformed public key"))?;

    Ok(Contact {
        identity,
        public_key,
        created_at: contact.created_at.unwrap_or_default(),
        first_name: contact.first_name,
        last_name: contact.last_name,
        nickname: contact.nickname,
        verification_level: contact
            .verification_level
            .and_then(|level| protobuf_contact::VerificationLevel::try_from(level).ok())
            .unwrap_or_default(),
        work_verification_level: contact
            .work_verification_level
            .and_then(|level| protobuf_contact::WorkVerificationLevel::try_from(level).ok())
            .unwrap_or_default(),
        identity_type: contact
            .identity_type
            .and_then(|kind| protobuf_contact::IdentityType::try_from(kind).ok())
            .unwrap_or_default(),
        acquaintance_level: contact
            .acquaintance_level
            .and_then(|level| protobuf_contact::AcquaintanceLevel::try_from(level).ok())
            .unwrap_or_default(),
        activity_state: contact
            .activity_state
            .and_then(|state| protobuf_contact::ActivityState::try_from(state).ok())
            .unwrap_or_default(),
        feature_mask: FeatureMask(contact.feature_mask.unwrap_or_default()),
        sync_state: contact
            .sync_state
            .and_then(|state| protobuf_contact::SyncState::try_from(state).ok())
            .unwrap_or_default(),
        // Work-only fields and per-contact policy overrides aren't used by this client (see the
        // same exclusions in `crate::store`).
        work_last_full_sync_at: None,
        work_availability_status: None,
        read_receipt_policy_override: None,
        typing_indicator_policy_override: None,
        notification_trigger_policy_override: None,
        conversation_category: Default::default(),
        conversation_visibility: Default::default(),
    })
}

/// Stores a contact a sibling device created or updated. This is the only way this client learns
/// contacts while a sibling holds the D2M leader role: the CSP receive path (which would look the
/// sender up in the directory and store it) never runs then, because the chat server delivers
/// messages to the leader only.
fn handle_contact_sync(contacts: &ContactStore, sync: protobuf::d2d::ContactSync) -> anyhow::Result<()> {
    use protobuf::d2d::contact_sync::Action;

    let (action, contact) = match sync.action {
        Some(Action::Create(create)) => ("create", create.contact),
        // An update is a delta and normally won't repeat the public key; it's only useful here
        // when we missed the create, which `decode_synced_contact` detects by the key's absence.
        Some(Action::Update(update)) => ("update", update.contact),
        None => return Ok(()),
    };
    let Some(contact) = contact else {
        return Ok(());
    };
    let identity = contact.identity.clone();
    let contact = match decode_synced_contact(contact) {
        Ok(contact) => contact,
        Err(error) => {
            tracing::debug!(action, identity, ?error, "Ignoring synced contact");
            return Ok(());
        },
    };

    // The store is `Rc`-backed, so this clone is just another view onto the same file.
    let mut contacts = contacts.clone();
    if contacts.has(contact.identity)? {
        tracing::debug!(action, identity, "Synced contact is already known");
        return Ok(());
    }
    contacts.add(vec![contact])?;
    tracing::info!(action, identity, "Stored contact synced from another device");
    Ok(())
}

fn emit(events: &mpsc::UnboundedSender<Event>, message: TextMessage) {
    // The only send failure is a dropped receiver, i.e. the embedder is shutting down.
    if events.send(Event::TextMessage(message)).is_err() {
        tracing::warn!("Dropping reflected message event: the event receiver is gone");
    }
}

/// Decrypts and decodes a `Reflected.envelope`, emitting an [`Event`] if (and only if) it's a
/// plain-text message. Returns `Ok(())` for anything else (contact/group sync, non-text
/// messages, ...) -- those are silently out of scope, not errors.
pub fn handle_reflected_envelope(
    device_group_key: &DeviceGroupKey,
    envelope: &[u8],
    contacts: &ContactStore,
    events: &mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let plaintext = device_group_key
        .decrypt_reflected_envelope(envelope)
        .map_err(|_| anyhow::anyhow!("Decrypting reflected D2D envelope failed"))?;
    let envelope = Envelope::decode(plaintext.as_slice()).context("Failed to decode d2d.Envelope")?;

    match envelope.content {
        Some(Content::OutgoingMessage(message)) if is_text_type(message.r#type) => {
            let text = String::from_utf8(message.body).context("Outgoing message body is not valid UTF-8")?;
            emit(events, TextMessage {
                timestamp_ms: message.created_at,
                outgoing: true,
                conversation: conversation(contacts, message.conversation),
                sender_identity: None,
                sender_name: None,
                text,
            });
        },
        Some(Content::IncomingMessage(message)) if is_text_type(message.r#type) => {
            let text = String::from_utf8(message.body).context("Incoming message body is not valid UTF-8")?;
            let sender_name = message
                .sender_identity
                .parse::<ThreemaId>()
                .ok()
                .and_then(|identity| contact_name(contacts, identity));
            // A reflected incoming message carries no conversation; only for a one-to-one text is
            // it implied by the sender (a group text's group identity sits in the undecoded body).
            let conversation = match CspE2eMessageType::try_from(message.r#type) {
                Ok(CspE2eMessageType::Text) => Conversation::Contact {
                    identity: message.sender_identity.clone(),
                    name: sender_name.clone(),
                },
                _ => Conversation::Unknown,
            };
            emit(events, TextMessage {
                timestamp_ms: message.created_at,
                outgoing: false,
                conversation,
                sender_identity: Some(message.sender_identity),
                sender_name,
                text,
            });
        },
        Some(Content::ContactSync(sync)) => handle_contact_sync(contacts, sync)?,
        other => {
            // A non-text message, or a sync content type we don't handle (group/settings sync,
            // profile sync, ...) -- out of scope, but worth naming while the client is young:
            // this is where a "why didn't X show up" investigation starts.
            tracing::debug!(content = content_name(other.as_ref()), "Ignoring reflected content");
        },
    }

    Ok(())
}

/// The variant name of some reflected content, for logging.
fn content_name(content: Option<&Content>) -> &'static str {
    match content {
        Some(Content::OutgoingMessage(_)) => "OutgoingMessage",
        Some(Content::OutgoingMessageUpdate(_)) => "OutgoingMessageUpdate",
        Some(Content::IncomingMessage(_)) => "IncomingMessage",
        Some(Content::IncomingMessageUpdate(_)) => "IncomingMessageUpdate",
        Some(Content::UserProfileSync(_)) => "UserProfileSync",
        Some(Content::ContactSync(_)) => "ContactSync",
        Some(Content::GroupSync(_)) => "GroupSync",
        Some(Content::DistributionListSync(_)) => "DistributionListSync",
        Some(Content::SettingsSync(_)) => "SettingsSync",
        Some(Content::MdmParameterSync(_)) => "MdmParameterSync",
        None => "(empty)",
    }
}

#[cfg(test)]
mod tests {
    use aead::{Aead as _, KeyInit as _, rand_core::RngCore as _};
    use blake2::{
        Blake2bMac,
        digest::{FixedOutput as _, consts::U32},
    };
    use crypto_secretbox::XSalsa20Poly1305;
    use libthreema::protobuf::{
        common::CspE2eMessageType,
        d2d::{ConversationId, OutgoingMessage, conversation_id::Id},
    };

    use super::*;

    /// Independent reimplementation of the DGRK derivation, used only to build a realistic
    /// encrypted test fixture -- cross-checked against the real
    /// `DeviceGroupKey::decrypt_reflected_envelope` below, not just self-consistency.
    fn derive_reflect_key(device_group_key: &[u8; 32]) -> [u8; 32] {
        let mac = Blake2bMac::<U32>::new_with_salt_and_personal(Some(device_group_key), b"r", b"3ma-mdev")
            .expect("Blake2bMac256 with a 32-byte key, 1-byte salt and 8-byte personalization should be valid");
        mac.finalize_fixed().into()
    }

    fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
        let mut nonce = [0_u8; 24];
        aead::rand_core::OsRng.fill_bytes(&mut nonce);
        let ciphertext = XSalsa20Poly1305::new(key.into())
            .encrypt((&nonce).into(), plaintext)
            .expect("encryption should succeed");
        [nonce.to_vec(), ciphertext].concat()
    }

    #[test]
    #[allow(deprecated, reason = "padding content is irrelevant for this round-trip test")]
    fn reflected_text_message_round_trips() {
        let device_group_key_bytes = [7_u8; 32];
        let reflect_key = derive_reflect_key(&device_group_key_bytes);

        let envelope = Envelope {
            padding: vec![],
            device_id: 1,
            protocol_version: 1,
            content: Some(Content::OutgoingMessage(OutgoingMessage {
                conversation: Some(ConversationId {
                    id: Some(Id::Contact("ECHOECHO".to_owned())),
                }),
                message_id: 42,
                thread_message_id: None,
                created_at: 1_700_000_000_000,
                r#type: CspE2eMessageType::Text as i32,
                body: b"hello from the phone".to_vec(),
                nonces: vec![],
            })),
        };
        let encrypted = encrypt(&reflect_key, &envelope.encode_to_vec());

        // Wrong key must not decrypt.
        let wrong_key = DeviceGroupKey::from([9_u8; 32]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        assert!(wrong_key.decrypt_reflected_envelope(&encrypted).is_err());

        // Right key, decrypted via the real (libthreema) implementation, must decrypt -- and the
        // whole pipeline should emit the message as an event.
        let device_group_key = DeviceGroupKey::from(device_group_key_bytes);
        let contacts_path = std::env::temp_dir().join(format!(
            "threema-cli-test-d2d-contacts-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after the epoch")
                .as_nanos()
        ));
        let contacts = ContactStore::load(contacts_path.clone(), "USERUSER".parse().expect("valid identity"))
            .expect("load empty contact store");
        handle_reflected_envelope(&device_group_key, &encrypted, &contacts, &events_tx)
            .expect("decrypting and decoding a well-formed envelope should succeed");
        let _ = std::fs::remove_file(&contacts_path);

        let Event::TextMessage(message) = events_rx.try_recv().expect("an event should have been emitted")
        else {
            panic!("the emitted event should be a text message");
        };
        assert!(message.outgoing);
        assert_eq!(message.timestamp_ms, 1_700_000_000_000);
        assert_eq!(message.text, "hello from the phone");
        assert!(
            matches!(message.conversation, Conversation::Contact { ref identity, .. } if identity == "ECHOECHO")
        );
    }

    /// While a sibling device holds the D2M leader role -- the normal case with a phone as the
    /// primary device -- the CSP receive path never runs here, so a reflected contact sync is the
    /// only way this client learns a contact's public key. Without it, sending fails with
    /// "not a known contact".
    #[test]
    #[allow(deprecated, reason = "padding content is irrelevant for this round-trip test")]
    fn reflected_contact_sync_stores_the_contact() {
        use libthreema::protobuf::{
            d2d::{ContactSync, contact_sync},
            d2d_sync,
        };

        let device_group_key_bytes = [7_u8; 32];
        let reflect_key = derive_reflect_key(&device_group_key_bytes);
        let public_key = [3_u8; 32];

        let envelope = Envelope {
            padding: vec![],
            device_id: 1,
            protocol_version: 1,
            content: Some(Content::ContactSync(ContactSync {
                action: Some(contact_sync::Action::Create(contact_sync::Create {
                    contact: Some(d2d_sync::Contact {
                        identity: "ECHOECHO".to_owned(),
                        public_key: Some(public_key.to_vec()),
                        nickname: Some("Echo".to_owned()),
                        ..Default::default()
                    }),
                })),
            })),
        };
        let encrypted = encrypt(&reflect_key, &envelope.encode_to_vec());

        let device_group_key = DeviceGroupKey::from(device_group_key_bytes);
        let contacts_path = std::env::temp_dir().join(format!(
            "threema-cli-test-d2d-sync-contacts-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after the epoch")
                .as_nanos()
        ));
        let contacts = ContactStore::load(contacts_path.clone(), "USERUSER".parse().expect("valid identity"))
            .expect("load empty contact store");
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        handle_reflected_envelope(&device_group_key, &encrypted, &contacts, &events_tx)
            .expect("decoding a well-formed contact sync should succeed");

        let identity: ThreemaId = "ECHOECHO".parse().expect("valid identity");
        let stored = contacts
            .get(identity)
            .expect("contact store lookup should succeed")
            .expect("the synced contact should have been stored");
        assert_eq!(stored.public_key.0.to_bytes(), public_key);
        assert_eq!(stored.nickname.as_deref(), Some("Echo"));
        // Storing a contact is not a user-visible message.
        assert!(events_rx.try_recv().is_err());

        // A second sync for the same contact must not fail (or clobber anything).
        handle_reflected_envelope(&device_group_key, &encrypted, &contacts, &events_tx)
            .expect("a repeated contact sync should be harmless");
        let _ = std::fs::remove_file(&contacts_path);
    }
}
