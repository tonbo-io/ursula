use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use ursula_headers::ClusterId;
use ursula_headers::Offset;

use super::ImportError;
use super::ImportOptions;
use super::NotNew;
use super::SourceProblem;
use super::Staged;
use super::check_header;
use super::run;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::snapshot::check_file;
use crate::codec::snapshot::read_image;
use crate::model::Limits;
use crate::model::ModelServer;
use crate::model::faults::Fault;
use crate::model::faults::Scripted;
use crate::options::OptionsError;
use crate::snapshotter::SnapshotFailure;
use crate::testdir::TempDir;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::transport::model::ModelTransport;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

const PATH: &str = "/v1/stream/b/imported";

fn server() -> Arc<Mutex<ModelServer>> {
    Arc::new(Mutex::new(ModelServer::new(
        ClusterId::new("cluster-a").unwrap(),
        7,
    )))
}

/// A plain database image of `pages` pages of 4096 bytes, its header counting them.
fn image(pages: u32) -> Vec<u8> {
    // Bytes that do not compress, so the body spans about as many bytes as the image.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut image: Vec<u8> = (0..pages as usize * 4096)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    image[..16].copy_from_slice(&SQLITE_MAGIC);
    image[16..18].copy_from_slice(&4096u16.to_be_bytes());
    // Rollback journal mode: the copy must be marked WAL.
    image[18] = 1;
    image[19] = 1;
    image[24..28].copy_from_slice(&7u32.to_be_bytes());
    image[28..32].copy_from_slice(&pages.to_be_bytes());
    image[92..96].copy_from_slice(&7u32.to_be_bytes());
    image
}

fn write_db(dir: &Path, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.join("plain.db");
    std::fs::write(&path, bytes).unwrap();
    path
}

fn deadline() -> Instant {
    Instant::now().checked_add(Duration::from_secs(30)).unwrap()
}

/// The image the stream's snapshot holds, read back as a build installs it.
fn snapshot_image(server: &Arc<Mutex<ModelServer>>) -> (Offset, Vec<u8>) {
    let head = head(server);
    let snapshot = head.snapshot().unwrap();
    let request = Request::snapshot_read(
        snapshot.offset,
        head.incarnation(),
        crate::wire::SnapshotPart::Chunk {
            first: 0,
            last: u64::MAX - 1,
        },
    );
    let answer = server
        .lock()
        .unwrap()
        .handle(PATH, &request)
        .parse(&request);
    assert_eq!(answer.status, 206);
    let mut body = std::io::Cursor::new(answer.body);
    let (header, at) =
        check_file(CONTENT_TYPE, &mut body, snapshot.offset, &snapshot.digest).unwrap();
    body.set_position(at as u64);
    let mut out = Vec::new();
    read_image(&header, &mut body, &mut out).unwrap();
    assert_eq!(header.fields.epoch.get(), 1);
    assert_eq!(header.fields.last.position.0, 0);
    (snapshot.offset, out)
}

fn head(server: &Arc<Mutex<ModelServer>>) -> ursula_headers::StreamHead {
    let request = Request::new(Method::Head, Target::Stream);
    match server
        .lock()
        .unwrap()
        .handle(PATH, &request)
        .parse(&request)
        .typed
    {
        Ok(Typed::Head {
            stream: Some(head), ..
        }) => head,
        other => panic!("{other:?}"),
    }
}

fn import(transport: &dyn Transport, path: &Path) -> Result<super::Imported, ImportError> {
    let staged = Staged::copy(path)?;
    run(transport, &staged, deadline(), 30_000)
}

#[test]
fn a_header_must_name_whole_4096_byte_pages() {
    let p = Path::new("x.db");
    assert_eq!(check_header(p, &image(3)[..100], 3 * 4096).unwrap(), 3);
    // A header whose page count is not current counts nothing: whole pages suffice.
    let mut stale = image(3);
    stale[92..96].copy_from_slice(&6u32.to_be_bytes());
    assert_eq!(check_header(p, &stale[..100], 5 * 4096).unwrap(), 5);
    assert!(matches!(
        check_header(p, &image(3)[..100], 4 * 4096),
        Err(SourceProblem::Length { pages: 3, .. })
    ));
    assert!(matches!(
        check_header(p, &image(3)[..100], 3 * 4096 + 1),
        Err(SourceProblem::PartialPage { .. })
    ));
    let mut big = image(3);
    big[16..18].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        check_header(p, &big[..100], 3 * 4096),
        Err(SourceProblem::PageSize {
            page_size: 65_536,
            ..
        })
    ));
    let mut small = image(3);
    small[16..18].copy_from_slice(&1024u16.to_be_bytes());
    assert!(matches!(
        check_header(p, &small[..100], 3 * 4096),
        Err(SourceProblem::PageSize {
            page_size: 1024,
            ..
        })
    ));
    assert!(matches!(
        check_header(p, &[0; 100], 4096),
        Err(SourceProblem::NotSqlite(_))
    ));
    assert!(matches!(
        check_header(p, &image(1)[..50], 4096),
        Err(SourceProblem::NotSqlite(_))
    ));
}

#[test]
fn options_take_a_token_alone() {
    assert_eq!(
        ImportOptions::try_from("").unwrap(),
        ImportOptions::default()
    );
    assert_eq!(
        ImportOptions::try_from(r#"{"token": "t"}"#)
            .unwrap()
            .token
            .as_deref(),
        Some("t")
    );
    assert_eq!(
        ImportOptions::try_from(r#"{"recreate": true}"#).unwrap_err(),
        OptionsError::Unknown("recreate".to_owned())
    );
}

// A WAL or a hot journal beside the file refuses it before anything is sent, and the import
// never writes the source (I1).
#[test]
fn a_wal_or_a_hot_journal_beside_the_file_refuses_it() {
    let dir = TempDir::new("import-beside");
    let path = write_db(dir.path(), &image(2));
    let wal = dir.path().join("plain.db-wal");
    std::fs::write(&wal, b"").unwrap();
    assert!(matches!(
        Staged::copy(&path),
        Err(ImportError::Source(SourceProblem::Wal(_)))
    ));
    std::fs::remove_file(&wal).unwrap();
    let journal = dir.path().join("plain.db-journal");
    std::fs::write(&journal, b"x").unwrap();
    assert!(matches!(
        Staged::copy(&path),
        Err(ImportError::Source(SourceProblem::HotJournal(_)))
    ));
    // An empty journal is not hot.
    std::fs::write(&journal, b"").unwrap();
    let staged = Staged::copy(&path).unwrap();
    assert_eq!(staged.pages, 2);
    let copy = std::fs::read(&staged.image).unwrap();
    assert_eq!(&copy[18..20], &[2, 2], "page 1 of the copy is marked WAL");
    assert_eq!(copy[20..], image(2)[20..]);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        image(2),
        "the source is unchanged"
    );
}

// A symbolic link to the file is resolved first: the WAL beside its target refuses it.
#[test]
fn a_wal_beside_the_target_of_a_link_refuses_it() {
    let dir = TempDir::new("import-link");
    let path = write_db(dir.path(), &image(2));
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    std::fs::write(dir.path().join("plain.db-wal"), b"").unwrap();
    assert!(matches!(
        Staged::copy(&link),
        Err(ImportError::Source(SourceProblem::Wal(_)))
    ));
}

// The import's claim carries the reserved activation, so no read of the stream builds from it.
#[test]
fn the_claim_carries_the_import_activation() {
    let dir = TempDir::new("import-activation");
    let path = write_db(dir.path(), &image(2));
    let server = server();
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let imported = import(&transport, &path).unwrap();
    let frames = server.lock().unwrap().frames(PATH).unwrap().to_vec();
    assert_eq!(imported.offset.get(), frames.len() as u64);
    assert_eq!(
        frames,
        frame::encode_claim(
            ursula_headers::Epoch::try_from(1).unwrap(),
            frame::IMPORT_ACTIVATION
        )
    );
}

// The import of a plain file into a missing stream: created, claimed at epoch 1 from offset 0,
// and its snapshot published at W, the claim's end, above 0, with the tail still at W. The
// snapshot's image is the file, page 1 marked WAL. A second import into the stream is refused
// stream_state and changes nothing (I2).
#[test]
fn a_file_becomes_the_first_snapshot_of_a_new_stream() {
    let dir = TempDir::new("import-single");
    let path = write_db(dir.path(), &image(5));
    let server = server();
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let imported = import(&transport, &path).unwrap();
    let frames = server.lock().unwrap().frames(PATH).unwrap().to_vec();
    assert!(imported.offset > Offset::ZERO);
    assert_eq!(imported.offset.get(), frames.len() as u64);
    assert_eq!(
        frames,
        frame::encode_claim(
            ursula_headers::Epoch::try_from(1).unwrap(),
            match frame::decode(&frames, 1 << 20) {
                Ok(frame::Decoded::Frame {
                    frame: frame::Frame::Claim { activation, .. },
                    ..
                }) => activation,
                other => panic!("{other:?}"),
            }
        )
    );
    assert_eq!(imported.pages, 5);
    let (offset, got) = snapshot_image(&server);
    assert_eq!(offset, imported.offset);
    let mut expected = image(5);
    expected[18] = 2;
    expected[19] = 2;
    assert_eq!(got, expected);
    let before = head(&server);
    let e = import(&transport, &path).unwrap_err();
    assert!(
        matches!(e, ImportError::NotNew(NotNew::HoldsSnapshot { .. })),
        "{e}"
    );
    assert_eq!(e.kind().as_str(), "stream_state");
    assert_eq!(head(&server), before);
    assert_eq!(server.lock().unwrap().frames(PATH).unwrap(), &frames[..]);
}

// Above the request limit, with a cold store, the body goes up in parts.
#[test]
fn a_file_above_the_request_limit_goes_up_in_parts() {
    let dir = TempDir::new("import-parts");
    let path = write_db(dir.path(), &image(64));
    let server = server();
    server.lock().unwrap().set_limits(Limits {
        max_request_bytes: 4096,
        max_snapshot_bytes: Some(1 << 30),
        ..Limits::default()
    });
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let imported = import(&transport, &path).unwrap();
    assert!(imported.bytes > 4096, "{imported:?}");
    let (offset, got) = snapshot_image(&server);
    assert_eq!(offset, imported.offset);
    assert_eq!(got[20..], image(64)[20..]);
    assert_eq!(server.lock().unwrap().staged_parts(), 0);
}

// Without a cold store, a body above the limit is refused naming it: the stream keeps its claim
// alone.
#[test]
fn a_file_above_the_limit_without_parts_is_refused_naming_it() {
    let dir = TempDir::new("import-too-large");
    let path = write_db(dir.path(), &image(64));
    let server = server();
    server.lock().unwrap().set_limits(Limits {
        max_request_bytes: 4096,
        ..Limits::default()
    });
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let e = import(&transport, &path).unwrap_err();
    assert!(
        matches!(
            e,
            ImportError::Publish(crate::snapshotter::SnapshotFailure::TooLarge {
                bound: crate::snapshotter::SizeBound::NoParts { request: 4096 },
                ..
            })
        ),
        "{e}"
    );
    assert!(e.to_string().contains("4096"), "{e}");
    assert_eq!(head(&server).snapshot(), None);
}

// An empty stream of the VFS's format is taken; one holding bytes, another content type or a
// closed one is refused with nothing sent after the HEAD.
#[test]
fn only_a_missing_or_empty_stream_is_imported_into() {
    let dir = TempDir::new("import-existing");
    let path = write_db(dir.path(), &image(2));
    let create = |content_type: &str| Request {
        content_type: Some(content_type.to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    };
    let server = server();
    server.lock().unwrap().handle(PATH, &create(CONTENT_TYPE));
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    import(&transport, &path).unwrap();

    let server = self::server();
    server.lock().unwrap().handle(PATH, &create(CONTENT_TYPE));
    server
        .lock()
        .unwrap()
        .foreign_append(PATH, &[1; 10])
        .unwrap();
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let e = import(&transport, &path).unwrap_err();
    assert!(
        matches!(e, ImportError::NotNew(NotNew::HoldsBytes { .. })),
        "{e}"
    );
    assert_eq!(server.lock().unwrap().frames(PATH).unwrap(), &[1; 10]);

    let server = self::server();
    server
        .lock()
        .unwrap()
        .handle(PATH, &create("application/octet-stream"));
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let e = import(&transport, &path).unwrap_err();
    assert_eq!(e.kind().as_str(), "stream_format", "{e}");
}

/// A change to the server before a request reaches it.
type ServerHook = Box<dyn FnMut(&mut ModelServer) + Send>;

/// A transport to the model that runs a hook on the server before each request `when` matches.
struct Hooked {
    server: Arc<Mutex<ModelServer>>,
    inner: ModelTransport,
    when: fn(&Request) -> bool,
    hook: Mutex<ServerHook>,
    answer: Option<fn(&Request) -> Option<RawAnswer>>,
}

impl Hooked {
    fn new(
        server: &Arc<Mutex<ModelServer>>,
        when: fn(&Request) -> bool,
        hook: impl FnMut(&mut ModelServer) + Send + 'static,
    ) -> Self {
        Self {
            server: Arc::clone(server),
            inner: ModelTransport::new(Arc::clone(server), PATH),
            when,
            hook: Mutex::new(Box::new(hook)),
            answer: None,
        }
    }
}

impl Transport for Hooked {
    fn send(&self, request: &Request, attempt: Attempt) -> Exchange {
        if (self.when)(request) {
            (self.hook.lock().unwrap())(&mut self.server.lock().unwrap());
        }
        if let Some(raw) = self.answer.and_then(|answer| answer(request)) {
            return Exchange::Answered(Box::new(raw.parse(request)));
        }
        self.inner.send(request, attempt)
    }
}

fn is_claim(request: &Request) -> bool {
    request.method == Method::Post && request.keys.producer.is_some()
}

// Bytes that land before the claim, or a claim of another importer, refuse the import before
// anything is published (I2).
#[test]
fn a_claim_that_is_not_first_publishes_nothing() {
    let dir = TempDir::new("import-race");
    let path = write_db(dir.path(), &image(2));
    let server = server();
    let mut once = true;
    let transport = Hooked::new(&server, is_claim, move |server| {
        if std::mem::take(&mut once) {
            server.foreign_append(PATH, &[1; 10]).unwrap();
        }
    });
    let e = import(&transport, &path).unwrap_err();
    assert!(
        matches!(e, ImportError::NotNew(NotNew::ClaimNotFirst { start }) if start.get() == 10),
        "{e}"
    );
    assert_eq!(head(&server).snapshot(), None);

    let server = self::server();
    let mut once = true;
    let transport = Hooked::new(&server, is_claim, move |server| {
        if std::mem::take(&mut once) {
            let epoch = ursula_headers::Epoch::try_from(2).unwrap();
            let body = frame::encode_claim(epoch, crate::ids::Activation([9; 16]));
            server.foreign_claim(PATH, epoch, &body).unwrap();
        }
    });
    let e = import(&transport, &path).unwrap_err();
    assert!(
        matches!(e, ImportError::NotNew(NotNew::ClaimLost { .. })),
        "{e}"
    );
    assert_eq!(head(&server).snapshot(), None);
}

// A claim whose answer is lost goes out again unchanged, and its repeat receipt names the same
// range (I5).
#[test]
fn a_claim_whose_answer_is_lost_is_resent_unchanged() {
    let dir = TempDir::new("import-lost");
    let path = write_db(dir.path(), &image(2));
    let server = server();
    server.lock().unwrap().inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropAnswer,
        times: 1,
    });
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let imported = import(&transport, &path).unwrap();
    let frames = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    assert_eq!(
        imported.offset.get(),
        frames,
        "one claim frame, no duplicate"
    );
}

// Bytes appended after the claim fail the import stream_state (I4): before the publish, nothing
// is published; after it (the publish's own HEAD), the final HEAD finds the tail past W.
#[test]
fn bytes_after_the_claim_fail_the_import() {
    for (append_at, published) in [(2, false), (3, true)] {
        let dir = TempDir::new("import-tail");
        let path = write_db(dir.path(), &image(2));
        let server = server();
        let mut heads = 0;
        let transport = Hooked::new(
            &server,
            |r| r.method == Method::Head,
            move |server| {
                heads += 1;
                if heads == append_at {
                    server.foreign_append(PATH, &[1; 10]).unwrap();
                }
            },
        );
        let e = import(&transport, &path).unwrap_err();
        assert!(
            matches!(e, ImportError::NotNew(NotNew::TailMoved { .. })),
            "{e}"
        );
        assert_eq!(e.kind().as_str(), "stream_state");
        assert_eq!(head(&server).snapshot().is_some(), published);
    }
}

// A gateway's concealing 404 without `Ursula-Protocol` is no missing stream: no_server_identity,
// and nothing is created.
#[test]
fn a_head_404_without_the_protocol_creates_nothing() {
    let dir = TempDir::new("import-concealed");
    let path = write_db(dir.path(), &image(2));
    let server = server();
    let mut transport = Hooked::new(&server, |_| false, |_| {});
    transport.answer =
        Some(|request| (request.method == Method::Head).then(|| RawAnswer::new(404)));
    let e = import(&transport, &path).unwrap_err();
    assert!(matches!(e, ImportError::NoServerIdentity), "{e}");
    assert_eq!(e.kind().as_str(), "no_server_identity");
    assert!(server.lock().unwrap().frames(PATH).is_none());
}

/// A HEAD's `404` from a server of the release line `protocol`.
fn missing_on_line(protocol: u32) -> RawAnswer {
    let mut raw = RawAnswer::new(404);
    ursula_headers::ServerIdentity {
        protocol,
        cluster_id: ClusterId::new("cluster-a").unwrap(),
    }
    .render(raw.emit());
    raw
}

// A server of another release line (0.8 to 0.10 send 2, a newer one its line) is refused
// protocol_mismatch
// before anything is created.
#[test]
fn a_server_of_another_release_line_creates_nothing() {
    type HeadAnswer = fn(&Request) -> Option<RawAnswer>;
    let answers: [(u32, HeadAnswer); 2] = [
        (2, |request| {
            (request.method == Method::Head).then(|| missing_on_line(2))
        }),
        (crate::open::table::next_line(), |request| {
            (request.method == Method::Head)
                .then(|| missing_on_line(crate::open::table::next_line()))
        }),
    ];
    for (found, answer) in answers {
        let dir = TempDir::new("import-other-line");
        let path = write_db(dir.path(), &image(2));
        let server = server();
        let mut transport = Hooked::new(&server, |_| false, |_| {});
        transport.answer = Some(answer);
        let e = import(&transport, &path).unwrap_err();
        assert!(
            matches!(e, ImportError::ProtocolMismatch { found: f } if f == found),
            "{e}"
        );
        assert_eq!(e.kind().as_str(), "protocol_mismatch");
        assert!(server.lock().unwrap().frames(PATH).is_none());
    }
}

/// A transport to the model whose HEADs after the first, the import's own, are rewritten by
/// `rewrite`: the stream's server changes once the import has created and claimed it.
struct LaterHeads {
    server: Arc<Mutex<ModelServer>>,
    /// Whether the import's own HEAD went by.
    seen_first: Mutex<bool>,
    rewrite: fn(&mut RawAnswer),
}

impl Transport for LaterHeads {
    fn send(&self, request: &Request, _attempt: Attempt) -> Exchange {
        let mut raw = self.server.lock().unwrap().handle(PATH, request);
        if request.method == Method::Head
            && std::mem::replace(&mut *self.seen_first.lock().unwrap(), true)
        {
            (self.rewrite)(&mut raw);
        }
        Exchange::Answered(Box::new(raw.parse(request)))
    }
}

/// An import whose HEADs after its first are rewritten by `rewrite`: its error, and whether the
/// stream then holds a snapshot.
fn import_with_later_heads(rewrite: fn(&mut RawAnswer)) -> (ImportError, bool) {
    let dir = TempDir::new("import-publish-line");
    let path = write_db(dir.path(), &image(2));
    let server = server();
    let transport = LaterHeads {
        server: Arc::clone(&server),
        seen_first: Mutex::new(false),
        rewrite,
    };
    let e = import(&transport, &path).unwrap_err();
    let snapshot = head(&server).snapshot().is_some();
    (e, snapshot)
}

// The publish's HEAD takes the same identity check as the import's first: a server of another
// release line fails the import protocol_mismatch, and an answer without identity
// no_server_identity, with no snapshot published.
#[test]
fn a_publish_head_of_another_line_or_without_identity_fails_the_import() {
    let (e, snapshot) = import_with_later_heads(|raw| {
        raw.headers.retain(|(name, _)| *name != "ursula-protocol");
        raw.header(
            "ursula-protocol",
            crate::open::table::next_line().to_string(),
        );
    });
    assert!(
        matches!(
            e,
            ImportError::Publish(SnapshotFailure::ProtocolMismatch { found })
                if found == crate::open::table::next_line()
        ),
        "{e}"
    );
    assert_eq!(e.kind().as_str(), "protocol_mismatch");
    assert!(!snapshot);

    let (e, snapshot) = import_with_later_heads(|raw| {
        raw.headers
            .retain(|(name, _)| !matches!(*name, "ursula-protocol" | "ursula-cluster-id"));
    });
    assert!(
        matches!(e, ImportError::Publish(SnapshotFailure::NoServerIdentity)),
        "{e}"
    );
    assert_eq!(e.kind().as_str(), "no_server_identity");
    assert!(!snapshot);
}
