//! Persistent state: nonces (`nonces.json`) and contacts (`contacts.json`).
//!
//! Both are plain JSON, rewritten atomically (write to a temp file, then rename) on every
//! mutation. Data volumes here are tiny (a personal contact list, a nonce set that grows slowly
//! over the tool's lifetime), so a full-file rewrite per change is not a real cost.
use core::cell::RefCell;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::Context as _;
use data_encoding::HEXLOWER_PERMISSIVE;
use libthreema::{
    common::{FeatureMask, Nonce, ThreemaId, keys::PublicKey},
    model::{
        contact::{Contact, ContactUpdate},
        provider::{ContactProvider, NonceStorage, ProfilePicture, ProviderError},
    },
    protobuf::{self, d2d_sync::contact as protobuf_contact},
};
use serde::{Deserialize, Serialize};

fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name().and_then(|name| name.to_str()).unwrap_or("state")
    ));
    let file = fs::File::create(&tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    serde_json::to_writer_pretty(&file, value).context("Failed to serialize state")?;
    file.sync_all()?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("Failed to move {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn load_json<T: Default + for<'de> Deserialize<'de>>(path: &Path) -> anyhow::Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }
    let bytes = fs::read(path).with_context(|| format!("Failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("Failed to parse {}", path.display()))
}

// Nonces
// ------

#[derive(Default, Serialize, Deserialize)]
struct NonceFile {
    csp_e2e: HashSet<String>,
    d2x: HashSet<String>,
}

#[derive(Clone, Copy)]
pub enum NonceScope {
    CspE2e,
    D2x,
}

/// File-backed storage shared by both nonce scopes (they live in the same file, but are tracked
/// as two independent sets). Not itself a [`NonceStorage`] -- call [`NonceStore::scoped`] to get
/// one.
#[derive(Clone)]
pub struct NonceStore {
    data: Rc<RefCell<NonceFile>>,
    path: Rc<PathBuf>,
}
impl NonceStore {
    pub fn load(path: PathBuf) -> anyhow::Result<Self> {
        let data: NonceFile = load_json(&path)?;
        Ok(Self {
            data: Rc::new(RefCell::new(data)),
            path: Rc::new(path),
        })
    }

    /// Get a [`NonceStorage`] view scoped to either the CSP E2E or the D2X nonce set. Cheap to
    /// call repeatedly: the returned views share (and persist to) the same underlying file.
    #[must_use]
    pub fn scoped(&self, scope: NonceScope) -> ScopedNonceStore {
        ScopedNonceStore {
            data: Rc::clone(&self.data),
            path: Rc::clone(&self.path),
            scope,
        }
    }
}

pub struct ScopedNonceStore {
    data: Rc<RefCell<NonceFile>>,
    path: Rc<PathBuf>,
    scope: NonceScope,
}
impl ScopedNonceStore {
    fn save(&self) -> anyhow::Result<()> {
        atomic_write_json(&self.path, &*self.data.borrow())
    }
}
impl NonceStorage for ScopedNonceStore {
    fn has(&self, nonce: &Nonce) -> Result<bool, ProviderError> {
        let hex = HEXLOWER_PERMISSIVE.encode(&nonce.0);
        let data = self.data.borrow();
        Ok(match self.scope {
            NonceScope::CspE2e => data.csp_e2e.contains(&hex),
            NonceScope::D2x => data.d2x.contains(&hex),
        })
    }

    fn add_many(&mut self, nonces: Vec<Nonce>) -> Result<(), ProviderError> {
        {
            let mut data = self.data.borrow_mut();
            let set = match self.scope {
                NonceScope::CspE2e => &mut data.csp_e2e,
                NonceScope::D2x => &mut data.d2x,
            };
            set.extend(nonces.iter().map(|nonce| HEXLOWER_PERMISSIVE.encode(&nonce.0)));
        }
        self.save().map_err(|error| ProviderError::Foreign(error.to_string()))
    }
}

// Contacts
// --------

/// On-disk mirror of [`Contact`]. Enum fields are stored as their raw `i32` discriminant since
/// the prost-generated enum types don't derive `Serialize`/`Deserialize`. Work-only override
/// fields (Remote Secret / read-receipt / typing-indicator / notification-trigger overrides,
/// last full sync) aren't persisted -- this is a consumer-account client and those are always
/// unset in practice.
#[derive(Clone, Serialize, Deserialize)]
struct StoredContact {
    public_key: String,
    created_at: u64,
    first_name: Option<String>,
    last_name: Option<String>,
    nickname: Option<String>,
    verification_level: i32,
    work_verification_level: i32,
    identity_type: i32,
    acquaintance_level: i32,
    activity_state: i32,
    feature_mask: u64,
    sync_state: i32,
    conversation_category: i32,
    conversation_visibility: i32,
}
impl StoredContact {
    fn from_contact(contact: &Contact) -> Self {
        Self {
            public_key: contact.public_key.to_string(),
            created_at: contact.created_at,
            first_name: contact.first_name.clone(),
            last_name: contact.last_name.clone(),
            nickname: contact.nickname.clone(),
            verification_level: contact.verification_level as i32,
            work_verification_level: contact.work_verification_level as i32,
            identity_type: contact.identity_type as i32,
            acquaintance_level: contact.acquaintance_level as i32,
            activity_state: contact.activity_state as i32,
            feature_mask: contact.feature_mask.0,
            sync_state: contact.sync_state as i32,
            conversation_category: contact.conversation_category as i32,
            conversation_visibility: contact.conversation_visibility as i32,
        }
    }

    fn to_contact(&self, identity: ThreemaId) -> anyhow::Result<Contact> {
        Ok(Contact {
            identity,
            public_key: PublicKey::from_hex(&self.public_key)?,
            created_at: self.created_at,
            first_name: self.first_name.clone(),
            last_name: self.last_name.clone(),
            nickname: self.nickname.clone(),
            verification_level: protobuf_contact::VerificationLevel::try_from(self.verification_level)
                .unwrap_or_default(),
            work_verification_level: protobuf_contact::WorkVerificationLevel::try_from(
                self.work_verification_level,
            )
            .unwrap_or_default(),
            identity_type: protobuf_contact::IdentityType::try_from(self.identity_type).unwrap_or_default(),
            acquaintance_level: protobuf_contact::AcquaintanceLevel::try_from(self.acquaintance_level)
                .unwrap_or_default(),
            activity_state: protobuf_contact::ActivityState::try_from(self.activity_state).unwrap_or_default(),
            feature_mask: FeatureMask(self.feature_mask),
            sync_state: protobuf_contact::SyncState::try_from(self.sync_state).unwrap_or_default(),
            work_last_full_sync_at: None,
            work_availability_status: None,
            read_receipt_policy_override: None,
            typing_indicator_policy_override: None,
            notification_trigger_policy_override: None,
            conversation_category: protobuf::d2d_sync::ConversationCategory::try_from(
                self.conversation_category,
            )
            .unwrap_or_default(),
            conversation_visibility: protobuf::d2d_sync::ConversationVisibility::try_from(
                self.conversation_visibility,
            )
            .unwrap_or_default(),
        })
    }
}

fn apply_delta_string(field: &mut Option<String>, delta: libthreema::common::Delta<String>) {
    match delta {
        libthreema::common::Delta::Unchanged => {},
        libthreema::common::Delta::Update(value) => *field = Some(value),
        libthreema::common::Delta::Remove => *field = None,
    }
}

/// File-backed [`ContactProvider`]. Clone cheaply (it's `Rc`-backed) to hand out multiple views
/// over the same underlying file, e.g. one for the protocol context and one for display-name
/// lookups.
#[derive(Clone)]
pub struct ContactStore {
    data: Rc<RefCell<HashMap<String, StoredContact>>>,
    path: Rc<PathBuf>,
    user_identity: ThreemaId,
}
impl ContactStore {
    pub fn load(path: PathBuf, user_identity: ThreemaId) -> anyhow::Result<Self> {
        let data: HashMap<String, StoredContact> = load_json(&path)?;
        Ok(Self {
            data: Rc::new(RefCell::new(data)),
            path: Rc::new(path),
            user_identity,
        })
    }

    fn save(&self) -> anyhow::Result<()> {
        atomic_write_json(&self.path, &*self.data.borrow())
    }

    fn ensure_not_user(&self, identity: ThreemaId) -> Result<(), ProviderError> {
        if identity == self.user_identity {
            return Err(ProviderError::InvalidParameter(
                "Unexpected encounter of user's identity in contact provider".to_owned(),
            ));
        }
        Ok(())
    }
}
impl ContactProvider for ContactStore {
    fn is_explicitly_blocked(&self, identity: ThreemaId) -> Result<bool, ProviderError> {
        self.ensure_not_user(identity)?;
        // Blocking isn't exposed by this client.
        Ok(false)
    }

    fn is_member_of_active_group(&self, identity: ThreemaId) -> Result<bool, ProviderError> {
        self.ensure_not_user(identity)?;
        // Groups aren't implemented (not yet supported by libthreema's CLI-side providers either).
        Ok(false)
    }

    fn has(&self, identity: ThreemaId) -> Result<bool, ProviderError> {
        self.ensure_not_user(identity)?;
        Ok(self.data.borrow().contains_key(&identity.to_string()))
    }

    fn has_many(&self, identities: &[ThreemaId]) -> Result<usize, ProviderError> {
        identities.iter().try_fold(0_usize, |count, identity| {
            self.ensure_not_user(*identity)?;
            Ok(if self.data.borrow().contains_key(&identity.to_string()) {
                count + 1
            } else {
                count
            })
        })
    }

    fn get(&self, identity: ThreemaId) -> Result<Option<Contact>, ProviderError> {
        self.ensure_not_user(identity)?;
        let Some(stored) = self.data.borrow().get(&identity.to_string()).cloned() else {
            return Ok(None);
        };
        stored
            .to_contact(identity)
            .map(Some)
            .map_err(|error| ProviderError::Foreign(error.to_string()))
    }

    fn add(&mut self, contacts: Vec<Contact>) -> Result<(), ProviderError> {
        for contact in &contacts {
            self.ensure_not_user(contact.identity)?;
            if self.data.borrow().contains_key(&contact.identity.to_string()) {
                return Err(ProviderError::InvalidState(
                    "Contact to be added already exists".to_owned(),
                ));
            }
        }
        {
            let mut data = self.data.borrow_mut();
            for contact in &contacts {
                let _ = data.insert(contact.identity.to_string(), StoredContact::from_contact(contact));
            }
        }
        self.save().map_err(|error| ProviderError::Foreign(error.to_string()))
    }

    fn update(&mut self, contacts: Vec<ContactUpdate>) -> Result<(), ProviderError> {
        {
            let mut data = self.data.borrow_mut();
            for update in contacts {
                self.ensure_not_user(update.identity)?;
                let Some(stored) = data.get_mut(&update.identity.to_string()) else {
                    return Err(ProviderError::InvalidState(
                        "Contact to be updated does not exist".to_owned(),
                    ));
                };
                apply_delta_string(&mut stored.first_name, update.first_name);
                apply_delta_string(&mut stored.last_name, update.last_name);
                apply_delta_string(&mut stored.nickname, update.nickname);
                if let Some(value) = update.verification_level {
                    stored.verification_level = value as i32;
                }
                if let Some(value) = update.work_verification_level {
                    stored.work_verification_level = value as i32;
                }
                if let Some(value) = update.identity_type {
                    stored.identity_type = value as i32;
                }
                if let Some(value) = update.acquaintance_level {
                    stored.acquaintance_level = value as i32;
                }
                if let Some(value) = update.activity_state {
                    stored.activity_state = value as i32;
                }
                if let Some(value) = update.feature_mask {
                    stored.feature_mask = value.0;
                }
                if let Some(value) = update.sync_state {
                    stored.sync_state = value as i32;
                }
                if let Some(value) = update.conversation_category {
                    stored.conversation_category = value as i32;
                }
                if let Some(value) = update.conversation_visibility {
                    stored.conversation_visibility = value as i32;
                }
            }
        }
        self.save().map_err(|error| ProviderError::Foreign(error.to_string()))
    }

    fn get_contact_defined_profile_picture(
        &self,
        identity: ThreemaId,
    ) -> Result<Option<ProfilePicture>, ProviderError> {
        self.ensure_not_user(identity)?;
        if !self.data.borrow().contains_key(&identity.to_string()) {
            return Err(ProviderError::InvalidState(
                "Contact to retrieve the contact-defined profile picture from does not exist".to_owned(),
            ));
        }
        Ok(None)
    }

    fn get_user_defined_profile_picture(
        &self,
        identity: ThreemaId,
    ) -> Result<Option<ProfilePicture>, ProviderError> {
        self.ensure_not_user(identity)?;
        if !self.data.borrow().contains_key(&identity.to_string()) {
            return Err(ProviderError::InvalidState(
                "Contact to retrieve the user-defined profile picture from does not exist".to_owned(),
            ));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use core::str::FromStr as _;

    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after the epoch")
            .as_nanos();
        path.push(format!("threema-cli-test-{}-{name}-{nanos}", std::process::id()));
        path
    }

    fn test_contact(identity: &str) -> Contact {
        Contact {
            identity: ThreemaId::from_str(identity).expect("valid test identity"),
            public_key: PublicKey::from_hex(&"ab".repeat(32)).expect("valid test public key"),
            created_at: 1_700_000_000_000,
            first_name: Some("Ada".to_owned()),
            last_name: None,
            nickname: Some("adalovelace".to_owned()),
            verification_level: Default::default(),
            work_verification_level: Default::default(),
            identity_type: Default::default(),
            acquaintance_level: Default::default(),
            activity_state: Default::default(),
            feature_mask: FeatureMask(FeatureMask::GROUP_SUPPORT),
            sync_state: Default::default(),
            work_last_full_sync_at: None,
            work_availability_status: None,
            read_receipt_policy_override: None,
            typing_indicator_policy_override: None,
            notification_trigger_policy_override: None,
            conversation_category: Default::default(),
            conversation_visibility: Default::default(),
        }
    }

    #[test]
    fn contact_round_trips_through_disk() {
        let user = ThreemaId::from_str("USERUSER").expect("valid test identity");
        let path = temp_path("contacts.json");
        let contact = test_contact("FRIENDLY");

        let mut store = ContactStore::load(path.clone(), user).expect("load empty store");
        store.add(vec![contact.clone()]).expect("add contact");
        assert!(path.exists(), "add() should have written the file");

        // Load a fresh store from disk (as if the process had restarted) and check the contact
        // survived, including through a subsequent update.
        let mut reloaded = ContactStore::load(path.clone(), user).expect("reload store");
        let fetched = reloaded
            .get(contact.identity)
            .expect("get should succeed")
            .expect("contact should be present");
        assert_eq!(fetched.identity, contact.identity);
        assert_eq!(fetched.nickname, contact.nickname);
        assert_eq!(fetched.first_name, contact.first_name);
        assert_eq!(fetched.public_key.to_string(), contact.public_key.to_string());

        reloaded
            .update(vec![ContactUpdate {
                identity: contact.identity,
                first_name: libthreema::common::Delta::Remove,
                last_name: libthreema::common::Delta::Unchanged,
                nickname: libthreema::common::Delta::Update("newnick".to_owned()),
                verification_level: None,
                work_verification_level: None,
                identity_type: None,
                acquaintance_level: None,
                activity_state: None,
                feature_mask: None,
                sync_state: None,
                read_receipt_policy_override: libthreema::common::Delta::Unchanged,
                typing_indicator_policy_override: libthreema::common::Delta::Unchanged,
                notification_trigger_policy_override: libthreema::common::Delta::Unchanged,
                conversation_category: None,
                conversation_visibility: None,
                work_availability_status: None,
                work_last_full_sync_at: None,
            }])
            .expect("update contact");

        let after_update = ContactStore::load(path.clone(), user)
            .expect("reload store again")
            .get(contact.identity)
            .expect("get should succeed")
            .expect("contact should still be present");
        assert_eq!(after_update.first_name, None);
        assert_eq!(after_update.nickname, Some("newnick".to_owned()));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn nonces_round_trip_through_disk_and_stay_scoped() {
        let path = temp_path("nonces.json");
        let nonce_bytes = [7_u8; 24];

        let store = NonceStore::load(path.clone()).expect("load empty store");
        let mut csp_e2e = store.scoped(NonceScope::CspE2e);
        let d2x = store.scoped(NonceScope::D2x);

        assert!(!csp_e2e.has(&Nonce(nonce_bytes)).expect("has should succeed"));
        csp_e2e
            .add_many(vec![Nonce(nonce_bytes)])
            .expect("add_many should succeed");
        assert!(csp_e2e.has(&Nonce(nonce_bytes)).expect("has should succeed"));
        assert!(
            !d2x.has(&Nonce(nonce_bytes)).expect("has should succeed"),
            "nonce added to the CSP E2E scope must not leak into the D2X scope"
        );
        assert!(path.exists(), "add_many() should have written the file");

        let reloaded = NonceStore::load(path.clone()).expect("reload store");
        assert!(
            reloaded
                .scoped(NonceScope::CspE2e)
                .has(&Nonce(nonce_bytes))
                .expect("has should succeed"),
            "nonce should have survived a reload"
        );
        assert!(
            !reloaded
                .scoped(NonceScope::D2x)
                .has(&Nonce(nonce_bytes))
                .expect("has should succeed")
        );

        let _ = std::fs::remove_file(&path);
    }
}
