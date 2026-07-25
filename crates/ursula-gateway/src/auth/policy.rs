//! Static file-based bucket authorization policy.
//!
//! [`StaticPolicyAuthorizer`] is the useful local default for a shared
//! gateway without a hosted control plane: a TOML file declares each bucket's
//! owners and whether anonymous reads are allowed. Everything else — unknown
//! buckets, private buckets probed by strangers, writes without ownership —
//! answers [`AuthorizationDecision::ConcealAsNotFound`] so a private
//! resource's existence is not observable.
//!
//! ```toml
//! [[bucket]]
//! id = "tenant-a"
//! public_read = true
//! owners = [{ issuer = "https://issuer.example", subject = "user-1" }]
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;

use super::AuthorizationDecision;
use super::AuthorizationFuture;
use super::AuthorizationRequest;
use super::Authorizer;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(default, rename = "bucket")]
    buckets: Vec<BucketPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BucketPolicy {
    id: String,
    #[serde(default)]
    public_read: bool,
    #[serde(default)]
    owners: Vec<OwnerRef>,
}

/// Owners are issuer-qualified subjects. A bare subject must never grant
/// access: `sub` values can collide between authorization servers.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerRef {
    issuer: String,
    subject: String,
}

#[derive(Debug, Default)]
struct BucketRule {
    public_read: bool,
    owners: HashSet<(String, String)>,
}

#[derive(Debug, Default)]
pub struct StaticPolicyAuthorizer {
    buckets: HashMap<String, BucketRule>,
}

impl StaticPolicyAuthorizer {
    pub fn from_toml_str(source: &str) -> Result<Self, PolicyError> {
        let file: PolicyFile =
            toml::from_str(source).map_err(|error| PolicyError::Parse(error.to_string()))?;
        let mut buckets = HashMap::new();
        for bucket in file.buckets {
            if bucket.id.is_empty() {
                return Err(PolicyError::EmptyBucketId);
            }
            let rule = BucketRule {
                public_read: bucket.public_read,
                owners: bucket
                    .owners
                    .into_iter()
                    .map(|owner| (owner.issuer, owner.subject))
                    .collect(),
            };
            if buckets.insert(bucket.id.clone(), rule).is_some() {
                return Err(PolicyError::DuplicateBucket(bucket.id));
            }
        }
        Ok(Self { buckets })
    }

    pub fn from_file(path: &Path) -> Result<Self, PolicyError> {
        let source = std::fs::read_to_string(path)
            .map_err(|error| PolicyError::Read(path.display().to_string(), error.to_string()))?;
        Self::from_toml_str(&source)
    }

    fn decide(&self, request: &AuthorizationRequest) -> AuthorizationDecision {
        let Some(rule) = self.buckets.get(&request.resource.bucket_id) else {
            return AuthorizationDecision::ConcealAsNotFound;
        };
        if let Some(principal) = &request.principal {
            let owner_key = (principal.issuer.clone(), principal.subject.clone());
            if rule.owners.contains(&owner_key) {
                return AuthorizationDecision::Allow;
            }
        }
        if rule.public_read && request.action.is_read_only() {
            return AuthorizationDecision::Allow;
        }
        AuthorizationDecision::ConcealAsNotFound
    }
}

impl Authorizer for StaticPolicyAuthorizer {
    fn authorize<'a>(&'a self, request: AuthorizationRequest) -> AuthorizationFuture<'a> {
        let decision = self.decide(&request);
        Box::pin(async move { Ok(decision) })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("failed to read policy file {0}: {1}")]
    Read(String, String),
    #[error("failed to parse policy TOML: {0}")]
    Parse(String),
    #[error("policy declares an empty bucket id")]
    EmptyBucketId,
    #[error("policy declares bucket {0:?} more than once")]
    DuplicateBucket(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::Action;
    use super::super::Resource;
    use super::super::VerifiedPrincipal;
    use super::*;

    const POLICY: &str = r#"
        [[bucket]]
        id = "tenant-a"
        public_read = true
        owners = [{ issuer = "https://issuer.example", subject = "user-a" }]

        [[bucket]]
        id = "tenant-b"
        owners = [{ issuer = "https://issuer.example", subject = "user-b" }]
    "#;

    fn principal(subject: &str) -> VerifiedPrincipal {
        VerifiedPrincipal {
            issuer: "https://issuer.example".to_owned(),
            subject: subject.to_owned(),
            client_id: "client".to_owned(),
            scopes: BTreeSet::new(),
            issued_at: 1,
            expires_at: u64::MAX,
            token_id: "token".to_owned(),
        }
    }

    fn request(
        principal_subject: Option<&str>,
        bucket: &str,
        action: Action,
    ) -> AuthorizationRequest {
        AuthorizationRequest {
            principal: principal_subject.map(principal),
            resource: Resource {
                bucket_id: bucket.to_owned(),
                stream_id: Some("orders".to_owned()),
            },
            action,
        }
    }

    fn decide(request: AuthorizationRequest) -> AuthorizationDecision {
        StaticPolicyAuthorizer::from_toml_str(POLICY)
            .expect("parse policy")
            .decide(&request)
    }

    #[test]
    fn owner_may_write_to_their_bucket() {
        assert_eq!(
            decide(request(Some("user-a"), "tenant-a", Action::Append)),
            AuthorizationDecision::Allow
        );
    }

    #[test]
    fn credential_does_not_cross_the_tenant_boundary() {
        assert_eq!(
            decide(request(Some("user-a"), "tenant-b", Action::Read)),
            AuthorizationDecision::ConcealAsNotFound
        );
        assert_eq!(
            decide(request(Some("user-a"), "tenant-b", Action::Append)),
            AuthorizationDecision::ConcealAsNotFound
        );
    }

    #[test]
    fn anonymous_read_is_allowed_only_on_public_buckets() {
        assert_eq!(
            decide(request(None, "tenant-a", Action::Read)),
            AuthorizationDecision::Allow
        );
        assert_eq!(
            decide(request(None, "tenant-a", Action::Tail)),
            AuthorizationDecision::Allow
        );
        assert_eq!(
            decide(request(None, "tenant-b", Action::Read)),
            AuthorizationDecision::ConcealAsNotFound
        );
    }

    #[test]
    fn public_read_does_not_grant_writes_or_administration() {
        assert_eq!(
            decide(request(None, "tenant-a", Action::Append)),
            AuthorizationDecision::ConcealAsNotFound
        );
        assert_eq!(
            decide(request(
                Some("user-b"),
                "tenant-a",
                Action::AdministerBucket
            )),
            AuthorizationDecision::ConcealAsNotFound
        );
    }

    #[test]
    fn unknown_bucket_is_concealed() {
        assert_eq!(
            decide(request(Some("user-a"), "tenant-c", Action::Read)),
            AuthorizationDecision::ConcealAsNotFound
        );
    }

    #[test]
    fn duplicate_bucket_ids_are_rejected() {
        let source = r#"
            [[bucket]]
            id = "tenant-a"
            [[bucket]]
            id = "tenant-a"
        "#;
        assert!(matches!(
            StaticPolicyAuthorizer::from_toml_str(source),
            Err(PolicyError::DuplicateBucket(_))
        ));
    }
}
