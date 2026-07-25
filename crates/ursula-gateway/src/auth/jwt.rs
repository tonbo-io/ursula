//! RFC 9068 JWT access-token validation.
//!
//! [`JwtPrincipalResolver`] validates `Bearer` credentials as OAuth 2.0 JWT
//! access tokens: the header must declare `typ: at+jwt`, the signature must
//! verify against a key from the authorization server's JWKS, and the standard
//! `iss`, `aud`, `sub`, `client_id`, `iat`, `exp`, and `jti` claims must all be
//! present and valid. OIDC ID tokens are rejected by the `typ` check.
//!
//! The JWKS location is either configured explicitly or discovered once from
//! RFC 8414 authorization-server metadata. Keys are cached by `kid`; an
//! unknown `kid` triggers a rate-limited refetch so key rotation does not
//! require a gateway restart.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use jsonwebtoken::decode;
use jsonwebtoken::decode_header;
use jsonwebtoken::jwk::AlgorithmParameters;
use jsonwebtoken::jwk::JwkSet;
use serde::Deserialize;
// tokio's Instant (not std's) so the DST scheduler can virtualize time if the
// gateway ever joins the simulation surface.
use tokio::time::Instant;

use super::AuthenticationError;
use super::PrincipalResolver;
use super::PrincipalResolverFuture;
use super::VerifiedPrincipal;
use super::parse_scope;

/// Signature algorithms accepted for access tokens. Symmetric algorithms are
/// excluded: a shared MAC secret cannot prove issuer identity to a resource
/// server.
const ALLOWED_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
];

const DEFAULT_JWKS_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_VALIDATION_LEEWAY_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct JwtValidationConfig {
    /// Expected `iss` claim, byte-for-byte.
    pub issuer: String,
    /// Expected `aud` claim identifying this resource server.
    pub audience: String,
    /// Explicit JWKS document URL. When absent the resolver performs RFC 8414
    /// metadata discovery under the issuer once and caches the result.
    pub jwks_url: Option<String>,
    /// Minimum interval between JWKS refetches triggered by unknown key IDs.
    pub jwks_refresh_min_interval: Duration,
}

impl JwtValidationConfig {
    pub fn new(issuer: impl Into<String>, audience: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            jwks_url: None,
            jwks_refresh_min_interval: DEFAULT_JWKS_REFRESH_MIN_INTERVAL,
        }
    }
}

/// The standard claims RFC 9068 requires in a JWT access token. Absence of any
/// field fails deserialization and therefore authentication.
#[derive(Debug, Deserialize)]
struct AccessTokenClaims {
    iss: String,
    sub: String,
    client_id: String,
    iat: u64,
    exp: u64,
    jti: String,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthorizationServerMetadata {
    jwks_uri: String,
}

#[derive(Clone)]
struct CachedKey {
    key: DecodingKey,
    algorithm: Option<Algorithm>,
}

#[derive(Default)]
struct KeyCache {
    keys: HashMap<String, CachedKey>,
}

/// Serializes JWKS discovery/refresh so concurrent unknown-`kid` requests do
/// not stampede the authorization server.
struct RefreshState {
    jwks_url: Option<String>,
    last_fetch: Option<Instant>,
}

pub struct JwtPrincipalResolver {
    config: JwtValidationConfig,
    http: reqwest::Client,
    keys: Mutex<KeyCache>,
    refresh: tokio::sync::Mutex<RefreshState>,
}

impl std::fmt::Debug for JwtPrincipalResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtPrincipalResolver")
            .field("issuer", &self.config.issuer)
            .field("audience", &self.config.audience)
            .finish_non_exhaustive()
    }
}

impl JwtPrincipalResolver {
    pub fn new(config: JwtValidationConfig) -> Result<Self, JwtResolverBuildError> {
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .build()
            .map_err(|error| JwtResolverBuildError::HttpClient(error.to_string()))?;
        let jwks_url = config.jwks_url.clone();
        Ok(Self {
            config,
            http,
            keys: Mutex::new(KeyCache::default()),
            refresh: tokio::sync::Mutex::new(RefreshState {
                jwks_url,
                last_fetch: None,
            }),
        })
    }

    async fn resolve_token(&self, token: &str) -> Result<VerifiedPrincipal, AuthenticationError> {
        let header = decode_header(token).map_err(|_error| {
            // Malformed compact serialization or unparsable header.
            AuthenticationError::InvalidCredential
        })?;
        if !is_access_token_type(header.typ.as_deref()) {
            // RFC 9068 requires `typ: at+jwt`; OIDC ID tokens must never be
            // accepted as access tokens.
            return Err(AuthenticationError::InvalidCredential);
        }
        if !ALLOWED_ALGORITHMS.contains(&header.alg) {
            return Err(AuthenticationError::InvalidCredential);
        }
        let kid = header.kid.ok_or(AuthenticationError::InvalidCredential)?;

        let cached = self.lookup_key(&kid);
        let cached = match cached {
            Some(cached) => cached,
            None => {
                self.refresh_keys().await?;
                self.lookup_key(&kid)
                    .ok_or(AuthenticationError::InvalidCredential)?
            }
        };
        // A key published for one algorithm must not verify a token that
        // claims another: cross-algorithm confusion is a classic JWT attack.
        if cached
            .algorithm
            .is_some_and(|algorithm| algorithm != header.alg)
        {
            return Err(AuthenticationError::InvalidCredential);
        }

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(std::slice::from_ref(&self.config.issuer));
        validation.set_audience(std::slice::from_ref(&self.config.audience));
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.leeway = DEFAULT_VALIDATION_LEEWAY_SECS;

        let decoded =
            decode::<AccessTokenClaims>(token, &cached.key, &validation).map_err(|error| {
                match error.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                        AuthenticationError::Expired
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                        AuthenticationError::WrongAudience
                    }
                    _ => AuthenticationError::InvalidCredential,
                }
            })?;
        let claims = decoded.claims;

        Ok(VerifiedPrincipal {
            issuer: claims.iss,
            subject: claims.sub,
            client_id: claims.client_id,
            scopes: claims.scope.as_deref().map(parse_scope).unwrap_or_default(),
            issued_at: claims.iat,
            expires_at: claims.exp,
            token_id: claims.jti,
        })
    }

    fn lookup_key(&self, kid: &str) -> Option<CachedKey> {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys
            .get(kid)
            .cloned()
    }

    async fn refresh_keys(&self) -> Result<(), AuthenticationError> {
        let mut refresh = self.refresh.lock().await;
        if refresh
            .last_fetch
            .is_some_and(|at| at.elapsed() < self.config.jwks_refresh_min_interval)
        {
            // Another caller refreshed moments ago; reuse its outcome instead
            // of hammering the authorization server with every bad `kid`.
            return Ok(());
        }

        let jwks_url = match refresh.jwks_url.clone() {
            Some(url) => url,
            None => {
                let discovered = self.discover_jwks_url().await?;
                refresh.jwks_url = Some(discovered.clone());
                discovered
            }
        };
        let jwk_set = self.fetch_jwks(&jwks_url).await?;
        refresh.last_fetch = Some(Instant::now());
        drop(refresh);

        let mut keys = HashMap::new();
        for jwk in &jwk_set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                continue;
            };
            let Ok(key) = DecodingKey::from_jwk(jwk) else {
                // Skip unusable entries (unsupported key type or malformed
                // parameters) rather than failing the whole set.
                continue;
            };
            if matches!(jwk.algorithm, AlgorithmParameters::OctetKey(_)) {
                // Symmetric JWKS entries are never valid signature keys here.
                continue;
            }
            let algorithm = jwk
                .common
                .key_algorithm
                .and_then(algorithm_from_key_algorithm);
            keys.insert(kid, CachedKey { key, algorithm });
        }

        let mut cache = self
            .keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.keys = keys;
        Ok(())
    }

    async fn discover_jwks_url(&self) -> Result<String, AuthenticationError> {
        let metadata_url = rfc8414_metadata_url(&self.config.issuer);
        let metadata = self
            .http
            .get(&metadata_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| {
                tracing::warn!(error = %error, url = %metadata_url, "authorization-server metadata fetch failed");
                AuthenticationError::Unavailable
            })?
            .json::<AuthorizationServerMetadata>()
            .await
            .map_err(|error| {
                tracing::warn!(error = %error, url = %metadata_url, "authorization-server metadata is invalid");
                AuthenticationError::Unavailable
            })?;
        Ok(metadata.jwks_uri)
    }

    async fn fetch_jwks(&self, jwks_url: &str) -> Result<JwkSet, AuthenticationError> {
        self.http
            .get(jwks_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| {
                tracing::warn!(error = %error, url = %jwks_url, "JWKS fetch failed");
                AuthenticationError::Unavailable
            })?
            .json::<JwkSet>()
            .await
            .map_err(|error| {
                tracing::warn!(error = %error, url = %jwks_url, "JWKS document is invalid");
                AuthenticationError::Unavailable
            })
    }
}

impl PrincipalResolver for JwtPrincipalResolver {
    fn resolve<'a>(&'a self, bearer_token: &'a str) -> PrincipalResolverFuture<'a> {
        Box::pin(self.resolve_token(bearer_token))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JwtResolverBuildError {
    #[error("failed to build JWKS HTTP client: {0}")]
    HttpClient(String),
}

/// RFC 9068 section 2.1: the `typ` header is `at+jwt`, optionally with the
/// full media-type prefix. Comparison is case-insensitive per RFC 2045.
fn is_access_token_type(typ: Option<&str>) -> bool {
    typ.is_some_and(|value| {
        value.eq_ignore_ascii_case("at+jwt") || value.eq_ignore_ascii_case("application/at+jwt")
    })
}

/// RFC 8414 well-known location: the path component is inserted between the
/// host and any issuer path suffix.
fn rfc8414_metadata_url(issuer: &str) -> String {
    match issuer.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('/') {
            Some((host, path)) => {
                format!("{scheme}://{host}/.well-known/oauth-authorization-server/{path}")
            }
            None => format!("{issuer}/.well-known/oauth-authorization-server"),
        },
        None => format!("{issuer}/.well-known/oauth-authorization-server"),
    }
}

/// Maps a JWKS `alg` declaration to a verification algorithm, ignoring
/// non-signature and symmetric entries.
fn algorithm_from_key_algorithm(
    key_algorithm: jsonwebtoken::jwk::KeyAlgorithm,
) -> Option<Algorithm> {
    use jsonwebtoken::jwk::KeyAlgorithm;
    match key_algorithm {
        KeyAlgorithm::RS256 => Some(Algorithm::RS256),
        KeyAlgorithm::RS384 => Some(Algorithm::RS384),
        KeyAlgorithm::RS512 => Some(Algorithm::RS512),
        KeyAlgorithm::ES256 => Some(Algorithm::ES256),
        KeyAlgorithm::ES384 => Some(Algorithm::ES384),
        KeyAlgorithm::PS256 => Some(Algorithm::PS256),
        KeyAlgorithm::PS384 => Some(Algorithm::PS384),
        KeyAlgorithm::PS512 => Some(Algorithm::PS512),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use axum::Json;
    use axum::Router;
    use axum::routing::get;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use jsonwebtoken::encode;
    use serde_json::Value;
    use serde_json::json;

    use super::*;

    const TEST_KID: &str = "test-key";
    const TEST_RSA_N: &str = "vdByYevk8hCmPkkGtkD5v9oh3_voy8i144aJGfdkSmP04G_etAwgE0Vz7oWwjF6i60mlpZGqg5AuJtVljd9lnIImS8Y9KBe8CtPIqA5myNiEaQju45NgNuOKyr-Z-vsG13IGNguIWJEMQpf0EIX2im_OwcZ7zPj3hRSNOqZKbqpghj8YSCKAPgHqw1qbzDxwS9fzg5uRU__boI-epFgQ0rlphWkZeZIDAtVzmxuSg0yP12EwHtJMDGmS2j5vg7W5pqLbrG0OJsmEVvnAgBngzSB2paWIeb3rRKGONxwM_Dx7Af5jPHIp8bFArDM-UM2adzVysFsAYTC54a3i5exRpw";
    const TEST_RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC90HJh6+TyEKY+\nSQa2QPm/2iHf++jLyLXjhokZ92RKY/Tgb960DCATRXPuhbCMXqLrSaWlkaqDkC4m\n1WWN32WcgiZLxj0oF7wK08ioDmbI2IRpCO7jk2A244rKv5n6+wbXcgY2C4hYkQxC\nl/QQhfaKb87BxnvM+PeFFI06pkpuqmCGPxhIIoA+AerDWpvMPHBL1/ODm5FT/9ug\nj56kWBDSuWmFaRl5kgMC1XObG5KDTI/XYTAe0kwMaZLaPm+DtbmmotusbQ4myYRW\n+cCAGeDNIHalpYh5vetEoY43HAz8PHsB/mM8cinxsUCsMz5QzZp3NXKwWwBhMLnh\nreLl7FGnAgMBAAECggEAP3doF3fBiHKqs17FgMD/APgVpYfcUe8daiM8ylLe1MTR\nRw4Av+DiBK+PUOygmH64KMXqPg8TnYPi+pcVqrdMiWU3GtUA89vjwxcHG7IdCgDj\nXR9giPVpEVFJwfsIqFOw+O6mRwHaKArvt4CJWdEBG07BKieMk3+Xe4bgpgCeGJwJ\n1Lyw/Y4S8/BZPm4aY9jYVYYaBcjNRuUnXP9gvdGK2RRXnAG4t1ZTvxv5fN1+40Kz\n3k67iGfZX7dgmvoo9KAk11WkgA9dLPF4qdLhR8RVqeQi/8U6Mb3q1Z7ONyNZHegS\nT6gC2ZrMTCc5g6rsW1J7d3XvCLQJW/qbpe9fW5uNCQKBgQDgTpHockM/DjIBufTy\nEVXap+21Seu2+Ap7wId6pwuJeKUcJMYSuEvCvpTrNPnd46UMW19WIQ8TuxTnCczJ\nRD15aC3mwfHoO19qS1mPJRls2MHxa30cgsmmYndlEuS7bHcHf9bGPV1Wi0Q9obAi\nlvGoDpFxvlgRf018OgTj+yEw1QKBgQDYoj0ehN1MNwdIlEx6PbUnS08yiE0Kh6tr\nvirFT8lEcEYhzCXH5vBBNTJO4zeRB5xO+5vdV7DDnS1GFwtejfa1kP0PfE3sE1x8\nBgv8kOvZ/8YTcKqE6ESUeHETZp/30CzQ4+wep2tlDBQWjV0I2Bh0EAAXcEObaQgs\nfKULLCKWiwKBgD42bI+dCXu2szX5Xq+5ESfpRavvibogx7+VIb5qEHAbjyfkJy/P\n/+tOsr0d32OknQV1XlbkKmtdiymddTgpfidrNrf2+OJhfVBc/8UNFCU1ZW1RU80R\nlV5ZlyXofJpjNgxVb7tiD75OOCoj61dcqD/lcn+qvIB134biDLMy1vzVAoGASj7I\nTbZhleZiM6jH0Tlm5bG00e/O36YBxSpmxDsFEtSb5Kdv52QpwV92/3x2JdmC47rt\n/103csNiqdvqBJ0JCc9IO89xcVBtaQA1iXktrAgyHaWGe4iTQINK1chdWPRa97i1\nywe8EeSi2dvXH9nX/6cgMOhD83Z626xYcEzPCeMCgYAiLVd+U5OEzGJRVQObFdpM\nLE5Io7aqGpuKhKNU3Xry6gSIE+qZ0NDreXsTrQVyUPSRJGQp83GToBA8hKnFOqkO\nS7WTcp/h7fWUtLFJPng4P9JWEbYEEY08BJ0tLfFu7ipO2FfepR4EkJ1WgwbeAcyf\nZKA71XBWEYP8n1MYFmukhw==\n-----END PRIVATE KEY-----\n";

    struct JwksServer {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for JwksServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn jwks_document(kid: &str) -> Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "kid": kid,
                "use": "sig",
                "alg": "RS256",
                "n": TEST_RSA_N,
                "e": "AQAB",
            }]
        })
    }

    async fn spawn_jwks_server(document: Value) -> JwksServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind JWKS server");
        let addr: SocketAddr = listener.local_addr().expect("JWKS server local addr");
        let url = format!("http://{addr}");
        let jwks_url = format!("{url}/jwks.json");
        let app = Router::new()
            .route(
                "/jwks.json",
                get(move || {
                    let document = document.clone();
                    async move { Json(document) }
                }),
            )
            .route(
                "/.well-known/oauth-authorization-server",
                get(move || {
                    let jwks_url = jwks_url.clone();
                    async move { Json(json!({ "jwks_uri": jwks_url, "issuer": "unused" })) }
                }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve JWKS");
        });
        JwksServer { url, task }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs()
    }

    fn sign_token(typ: Option<&str>, kid: Option<&str>, claims: &Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.typ = typ.map(str::to_owned);
        header.kid = kid.map(str::to_owned);
        let key = EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("test RSA key");
        encode(&header, claims, &key).expect("sign test token")
    }

    fn standard_claims(expires_at: u64) -> Value {
        json!({
            "iss": "https://issuer.example",
            "aud": "https://streams.example",
            "sub": "user-1",
            "client_id": "client-1",
            "iat": now_secs(),
            "exp": expires_at,
            "jti": "token-1",
            "scope": "streams:read streams:write",
        })
    }

    fn resolver(jwks: &JwksServer) -> JwtPrincipalResolver {
        let mut config =
            JwtValidationConfig::new("https://issuer.example", "https://streams.example");
        config.jwks_url = Some(format!("{}/jwks.json", jwks.url));
        config.jwks_refresh_min_interval = Duration::ZERO;
        JwtPrincipalResolver::new(config).expect("build resolver")
    }

    #[tokio::test]
    async fn valid_access_token_resolves_to_a_standard_principal() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let token = sign_token(
            Some("at+jwt"),
            Some(TEST_KID),
            &standard_claims(now_secs() + 600),
        );

        let principal = resolver(&jwks)
            .resolve(&token)
            .await
            .expect("valid token resolves");
        assert_eq!(principal.issuer, "https://issuer.example");
        assert_eq!(principal.subject, "user-1");
        assert_eq!(principal.client_id, "client-1");
        assert_eq!(principal.token_id, "token-1");
        assert!(principal.has_scope("streams:write"));
    }

    #[tokio::test]
    async fn oidc_id_token_typ_is_rejected() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let token = sign_token(
            Some("JWT"),
            Some(TEST_KID),
            &standard_claims(now_secs() + 600),
        );

        assert_eq!(
            resolver(&jwks).resolve(&token).await,
            Err(AuthenticationError::InvalidCredential)
        );
    }

    #[tokio::test]
    async fn expired_token_reports_expiry() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let token = sign_token(
            Some("at+jwt"),
            Some(TEST_KID),
            &standard_claims(now_secs() - 600),
        );

        assert_eq!(
            resolver(&jwks).resolve(&token).await,
            Err(AuthenticationError::Expired)
        );
    }

    #[tokio::test]
    async fn token_for_another_resource_server_is_rejected() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let mut claims = standard_claims(now_secs() + 600);
        claims["aud"] = json!("https://other.example");
        let token = sign_token(Some("at+jwt"), Some(TEST_KID), &claims);

        assert_eq!(
            resolver(&jwks).resolve(&token).await,
            Err(AuthenticationError::WrongAudience)
        );
    }

    #[tokio::test]
    async fn missing_client_id_claim_is_rejected() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let mut claims = standard_claims(now_secs() + 600);
        claims
            .as_object_mut()
            .expect("claims object")
            .remove("client_id");
        let token = sign_token(Some("at+jwt"), Some(TEST_KID), &claims);

        assert_eq!(
            resolver(&jwks).resolve(&token).await,
            Err(AuthenticationError::InvalidCredential)
        );
    }

    #[tokio::test]
    async fn unknown_kid_refetches_jwks_and_supports_rotation() {
        let jwks = spawn_jwks_server(jwks_document("rotated-key")).await;
        let resolver = resolver(&jwks);

        let stale = sign_token(
            Some("at+jwt"),
            Some(TEST_KID),
            &standard_claims(now_secs() + 600),
        );
        assert_eq!(
            resolver.resolve(&stale).await,
            Err(AuthenticationError::InvalidCredential)
        );

        let rotated = sign_token(
            Some("at+jwt"),
            Some("rotated-key"),
            &standard_claims(now_secs() + 600),
        );
        let principal = resolver
            .resolve(&rotated)
            .await
            .expect("rotated key resolves after refetch");
        assert_eq!(principal.subject, "user-1");
    }

    #[tokio::test]
    async fn jwks_url_is_discovered_from_rfc8414_metadata() {
        let jwks = spawn_jwks_server(jwks_document(TEST_KID)).await;
        let mut config = JwtValidationConfig::new(jwks.url.clone(), "https://streams.example");
        config.jwks_refresh_min_interval = Duration::ZERO;
        let resolver = JwtPrincipalResolver::new(config).expect("build resolver");

        let mut claims = standard_claims(now_secs() + 600);
        claims["iss"] = json!(jwks.url.clone());
        let token = sign_token(Some("at+jwt"), Some(TEST_KID), &claims);

        let principal = resolver
            .resolve(&token)
            .await
            .expect("discovered JWKS resolves token");
        assert_eq!(principal.subject, "user-1");
    }

    #[test]
    fn metadata_url_inserts_well_known_between_host_and_path() {
        assert_eq!(
            rfc8414_metadata_url("https://issuer.example"),
            "https://issuer.example/.well-known/oauth-authorization-server"
        );
        assert_eq!(
            rfc8414_metadata_url("https://issuer.example/tenant-a"),
            "https://issuer.example/.well-known/oauth-authorization-server/tenant-a"
        );
    }
}
