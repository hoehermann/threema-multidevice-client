//! Decodes D2D `Reflected` envelopes -- the sync messages a *sibling* device (e.g. the phone,
//! whenever it -- not this CLI -- holds the D2M "leader" role) sends about messages it has sent or
//! received, so every linked device's history stays consistent.
//!
//! libthreema's `CspE2eProtocol` has no decoder for this (only the encode side, used when *this*
//! device is leader and needs to reflect to others -- see `csp_e2e::reflect`). Decryption itself
//! goes through `DeviceGroupKey::decrypt_reflected_envelope`, added directly to the vendored
//! libthreema (it already has the correct, tested DGRK derivation crate-private; exposing one
//! narrow method beats reimplementing that derivation independently). What's left to do here is
//! just parsing the decrypted bytes as a `d2d.Envelope` and printing `body` for the two content
//! variants that carry a plain-text message -- everything else (contact/group/settings sync,
//! etc.) is out of scope, same boundary as the CSP-E2E side only handling text messages.
use anyhow::Context as _;
use libthreema::{
    common::{GroupIdentity, ThreemaId, keys::DeviceGroupKey},
    protobuf::{
        self,
        common::CspE2eMessageType,
        d2d::{Envelope, envelope::Content},
    },
};
use prost::Message as _;

use crate::store::ContactStore;

fn conversation_label(contacts: &ContactStore, conversation: Option<protobuf::d2d::ConversationId>) -> String {
    use protobuf::d2d::conversation_id::Id;
    match conversation.and_then(|conversation| conversation.id) {
        Some(Id::Contact(identity)) => match identity.parse::<ThreemaId>() {
            Ok(identity) => crate::conversation::display_name(contacts, identity),
            Err(_) => identity,
        },
        Some(Id::Group(group)) => match GroupIdentity::try_from(&group) {
            Ok(group) => format!("group {}/{}", group.creator_identity, group.group_id),
            Err(_) => "group <invalid>".to_owned(),
        },
        Some(Id::DistributionList(id)) => format!("distribution list {id}"),
        None => "<unknown conversation>".to_owned(),
    }
}

fn is_text_type(message_type: i32) -> bool {
    matches!(
        CspE2eMessageType::try_from(message_type),
        Ok(CspE2eMessageType::Text | CspE2eMessageType::GroupText)
    )
}

/// Decrypts and decodes a `Reflected.envelope`, printing it if (and only if) it's a plain-text
/// message. Returns `Ok(())` for anything else (contact/group sync, non-text messages, ...) --
/// those are silently out of scope, not errors.
pub fn handle_reflected_envelope(
    device_group_key: &DeviceGroupKey,
    envelope: &[u8],
    contacts: &ContactStore,
) -> anyhow::Result<()> {
    let plaintext = device_group_key
        .decrypt_reflected_envelope(envelope)
        .map_err(|_| anyhow::anyhow!("Decrypting reflected D2D envelope failed"))?;
    let envelope = Envelope::decode(plaintext.as_slice()).context("Failed to decode d2d.Envelope")?;

    match envelope.content {
        Some(Content::OutgoingMessage(message)) if is_text_type(message.r#type) => {
            let text = String::from_utf8(message.body).context("Outgoing message body is not valid UTF-8")?;
            let to = conversation_label(contacts, message.conversation);
            println!("[{}] me (to {to}): {text}", message.created_at);
        },
        Some(Content::IncomingMessage(message)) if is_text_type(message.r#type) => {
            let text = String::from_utf8(message.body).context("Incoming message body is not valid UTF-8")?;
            let author = match message.sender_identity.parse::<ThreemaId>() {
                Ok(identity) => crate::conversation::display_name(contacts, identity),
                Err(_) => message.sender_identity,
            };
            println!("[{}] {author}: {text}", message.created_at);
        },
        _ => {
            // Non-text message, or a sync content type we don't handle (contact/group/settings
            // sync, profile sync, ...) -- out of scope.
        },
    }

    Ok(())
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
        assert!(wrong_key.decrypt_reflected_envelope(&encrypted).is_err());

        // Right key, decrypted via the real (libthreema) implementation, must decrypt -- and the
        // whole pipeline should run without error.
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
        handle_reflected_envelope(&device_group_key, &encrypted, &contacts)
            .expect("decrypting and decoding a well-formed envelope should succeed");
        let _ = std::fs::remove_file(&contacts_path);
    }
}
