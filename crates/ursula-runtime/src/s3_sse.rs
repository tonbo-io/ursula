//! Keeps server-side-encryption headers off the S3 multipart requests that
//! reject them.
//!
//! AWS S3 takes the SSE-S3 / SSE-KMS request headers
//! (`x-amz-server-side-encryption`, `...-aws-kms-key-id`, `...-context`,
//! `...-bucket-key-enabled`) on PutObject, CopyObject and
//! CreateMultipartUpload only: the upload's encryption is fixed when it is
//! created. UploadPart answers `400 InvalidArgument` ("x-amz-server-side-
//! encryption header is not supported for this operation") when one is
//! present, and CompleteMultipartUpload does not document them either. Only
//! the SSE-C `...-customer-*` headers belong on UploadPart (they are
//! required there) and CompleteMultipartUpload.
//!
//! opendal 0.51 (and upstream as of 0.59) adds the configured SSE headers to
//! every write request, multipart parts included, so with the default
//! `server_side_encryption = "aes256"` every multipart upload to real S3
//! failed. MinIO accepts the header, which is why only AWS showed it. The
//! request is already SigV4-signed (with the header among `SignedHeaders`)
//! when it reaches the HTTP client, so [`MultipartSseFetch`] strips the
//! headers there and signs the request again with the same credential chain.

use http::Method;
use http::Request;
use http::Response;
use http::header;
use opendal::Buffer;
use opendal::Error;
use opendal::ErrorKind;
use opendal::raw::HttpBody;
use opendal::raw::HttpClient;
use opendal::raw::HttpFetch;
use reqsign::AwsConfig;
use reqsign::AwsDefaultLoader;
use reqsign::AwsV4Signer;

/// Headers that fix an upload's SSE-S3 / SSE-KMS encryption: valid on
/// CreateMultipartUpload, rejected on the requests that continue it.
const UPLOAD_SCOPED_SSE_HEADERS: [&str; 4] = [
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-bucket-key-enabled",
];

/// An [`HttpFetch`] that removes [`UPLOAD_SCOPED_SSE_HEADERS`] from
/// UploadPart and CompleteMultipartUpload requests and re-signs them; every
/// other request passes through untouched.
pub(crate) struct MultipartSseFetch {
    inner: HttpClient,
    signer: AwsV4Signer,
    loader: AwsDefaultLoader,
}

impl MultipartSseFetch {
    /// Mirrors how opendal's S3 builder resolves region and credentials, so
    /// a re-signed request carries the same identity as the original.
    /// `None` when no region resolves: opendal then refuses to build the
    /// operator anyway.
    pub(crate) fn new(s3: &ursula_config::S3Config, inner: HttpClient) -> Option<Self> {
        let non_empty = |value: &Option<String>| {
            value
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        };
        let mut cfg = AwsConfig::default().from_profile().from_env();
        if let Some(region) = non_empty(&s3.region) {
            cfg.region = Some(region);
        }
        if let Some(access_key_id) = non_empty(&s3.access_key_id) {
            cfg.access_key_id = Some(access_key_id);
        }
        if let Some(secret_access_key) = non_empty(&s3.secret_access_key) {
            cfg.secret_access_key = Some(secret_access_key);
        }
        if let Some(session_token) = non_empty(&s3.session_token) {
            cfg.session_token = Some(session_token);
        }
        let region = cfg.region.clone()?;
        Some(Self {
            inner,
            signer: AwsV4Signer::new("s3", &region),
            loader: AwsDefaultLoader::new(reqwest::Client::new(), cfg),
        })
    }

    async fn resign(&self, req: &mut Request<Buffer>) -> opendal::Result<()> {
        let credential = self.loader.load().await.map_err(|err| {
            Error::new(
                ErrorKind::PermissionDenied,
                "load S3 credential to re-sign a multipart request",
            )
            .set_source(err)
            .set_temporary()
        })?;
        let Some(credential) = credential else {
            // The SSE headers are gone but the old signature still covers
            // them; sending it would fail with SignatureDoesNotMatch.
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "no S3 credential to re-sign a multipart request",
            )
            .set_temporary());
        };
        let headers = req.headers_mut();
        headers.remove(header::AUTHORIZATION);
        headers.remove("x-amz-date");
        headers.remove("x-amz-security-token");
        self.signer.sign(req, &credential).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "re-sign S3 multipart request").set_source(err)
        })?;
        // As opendal does after signing: let the HTTP client set Host.
        req.headers_mut().remove(header::HOST);
        Ok(())
    }
}

/// UploadPart (`PUT ?partNumber&uploadId`) or CompleteMultipartUpload
/// (`POST ?uploadId`).
fn continues_multipart_upload<T>(req: &Request<T>) -> bool {
    let query = req.uri().query().unwrap_or_default();
    let has = |key: &str| {
        query
            .split('&')
            .any(|pair| pair.split('=').next() == Some(key))
    };
    match *req.method() {
        Method::PUT => has("uploadId") && has("partNumber"),
        Method::POST => has("uploadId"),
        _ => false,
    }
}

impl HttpFetch for MultipartSseFetch {
    async fn fetch(&self, mut req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        if continues_multipart_upload(&req) {
            let mut stripped = false;
            for name in UPLOAD_SCOPED_SSE_HEADERS {
                stripped |= req.headers_mut().remove(name).is_some();
            }
            if stripped {
                self.resign(&mut req).await?;
            }
        }
        self.inner.fetch(req).await
    }
}

/// The production HTTP transport opendal would otherwise build for S3.
pub(crate) fn default_fetcher() -> HttpClient {
    HttpClient::with(reqwest::Client::new())
}
