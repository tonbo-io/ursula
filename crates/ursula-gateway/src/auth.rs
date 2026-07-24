//! Provider-neutral OAuth resource-server boundary.
//!
//! Authentication adapters validate a bearer credential and normalize only
//! standard OAuth access-token properties. Bucket selection stays explicit:
//! neither an OAuth subject nor a provider-private claim selects a namespace.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type PrincipalResolverFuture<'a> =
    Pin<Box<dyn Future<Output = Result<VerifiedPrincipal, AuthenticationError>> + Send + 'a>>;
pub type AuthorizationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AuthorizationDecision, AuthorizationError>> + Send + 'a>>;

/// A verified OAuth principal normalized from an RFC 9068 JWT access token or
/// an RFC 7662 token-introspection response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    pub issuer: String,
    pub subject: String,
    pub client_id: String,
    pub scopes: BTreeSet<String>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub token_id: String,
}

impl VerifiedPrincipal {
    /// The issuer-qualified subject is the stable principal key. A bare `sub`
    /// can collide between otherwise trusted authorization servers.
    pub fn principal_key(&self) -> PrincipalKey<'_> {
        PrincipalKey {
            issuer: &self.issuer,
            subject: &self.subject,
        }
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrincipalKey<'a> {
    pub issuer: &'a str,
    pub subject: &'a str,
}

pub fn parse_scope(scope: &str) -> BTreeSet<String> {
    scope
        .split_ascii_whitespace()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Credential verification only. Implementations may validate an RFC 9068 JWT
/// locally or call an RFC 7662 introspection endpoint for an opaque token.
pub trait PrincipalResolver: Send + Sync {
    fn resolve<'a>(&'a self, bearer_token: &'a str) -> PrincipalResolverFuture<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthenticationError {
    #[error("bearer credential is invalid")]
    InvalidCredential,
    #[error("bearer credential is expired")]
    Expired,
    #[error("bearer credential is not intended for this resource server")]
    WrongAudience,
    #[error("authentication service is unavailable")]
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Head,
    Tail,
    Append,
    AppendAndClose,
    Create,
    CreateAndClose,
    Update,
    Delete,
    PublishSnapshot,
    ReadSnapshot,
    DeleteSnapshot,
    AdministerBucket,
}

/// Two-level resource identity resolved independently from the credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource {
    /// The top-level namespace and logical tenant boundary.
    pub bucket_id: String,
    pub stream_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub principal: Option<VerifiedPrincipal>,
    pub resource: Resource,
    pub action: Action,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDecision {
    Allow,
    Deny,
    /// Return the same 404 surface as a missing private resource.
    ConcealAsNotFound,
}

/// Product-local membership and resource policy evaluation.
///
/// Implementations can call or cache a hosted control plane, use static local
/// policy, or allow every request in an explicit trusted deployment.
pub trait Authorizer: Send + Sync {
    fn authorize<'a>(&'a self, request: AuthorizationRequest) -> AuthorizationFuture<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationError {
    #[error("authorization service is unavailable")]
    Unavailable,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize<'a>(&'a self, _request: AuthorizationRequest) -> AuthorizationFuture<'a> {
        Box::pin(async { Ok(AuthorizationDecision::Allow) })
    }
}

/// Opt-in access-control hooks for a hosted or otherwise shared gateway.
///
/// The standalone gateway does not install this value and keeps its original
/// pass-through behavior. Deployments that do install it authenticate any
/// presented bearer credential, then authorize both anonymous and authenticated
/// requests against the explicit bucket/stream resource.
#[derive(Clone)]
pub struct AccessControl {
    principal_resolver: Arc<dyn PrincipalResolver>,
    authorizer: Arc<dyn Authorizer>,
}

impl std::fmt::Debug for AccessControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessControl").finish_non_exhaustive()
    }
}

impl AccessControl {
    pub fn new(
        principal_resolver: Arc<dyn PrincipalResolver>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Self {
        Self {
            principal_resolver,
            authorizer,
        }
    }

    pub(crate) fn principal_resolver(&self) -> &dyn PrincipalResolver {
        self.principal_resolver.as_ref()
    }

    pub(crate) fn authorizer(&self) -> &dyn Authorizer {
        self.authorizer.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(issuer: &str, subject: &str) -> VerifiedPrincipal {
        VerifiedPrincipal {
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            client_id: "client".to_owned(),
            scopes: parse_scope("streams:write streams:read streams:read"),
            issued_at: 1,
            expires_at: 2,
            token_id: "token".to_owned(),
        }
    }

    #[test]
    fn standard_scope_is_space_delimited_and_deduplicated() {
        let principal = principal("https://issuer.example", "same-subject");

        assert!(principal.has_scope("streams:read"));
        assert!(principal.has_scope("streams:write"));
        assert_eq!(principal.scopes.len(), 2);
    }

    #[test]
    fn principal_identity_is_issuer_qualified() {
        let first = principal("https://issuer-a.example", "same-subject");
        let second = principal("https://issuer-b.example", "same-subject");

        assert_ne!(first.principal_key(), second.principal_key());
    }

    #[tokio::test]
    async fn authorization_receives_bucket_independently_from_principal() {
        let request = AuthorizationRequest {
            principal: Some(principal("https://issuer.example", "user-1")),
            resource: Resource {
                bucket_id: "owner-2".to_owned(),
                stream_id: Some("orders".to_owned()),
            },
            action: Action::Read,
        };

        let decision = AllowAllAuthorizer
            .authorize(request)
            .await
            .expect("allow-all authorization");

        assert_eq!(decision, AuthorizationDecision::Allow);
    }
}
