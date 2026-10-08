"""Reproduce stale ACK handling against two unmodified published OpenRaft releases.

Usage: python3 scripts/regressions/openraft_stale_ack.py /absolute/scratch/path
Downloads crates.io archives into scratch; never edits Cargo's registry cache.
The alpha21 tests must fail; the same tests on alpha28 must pass.
"""
import io
import os
import re
from pathlib import Path
import subprocess
import sys
import tarfile

root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=True)
fixture = Path(__file__).with_suffix('.rs').read_text()
for alpha in (21, 28):
    version = f'0.10.0-alpha.{alpha}'
    source = root / f'openraft-{version}'
    if source.exists():
        raise SystemExit(f'Refusing to modify existing directory: {source}')
    archive = subprocess.check_output([
        'curl', '--fail', '--silent', '--show-error', '--location',
        f'https://static.crates.io/crates/openraft/openraft-{version}.crate',
    ])
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
        tar.extractall(root, filter='data')
    test = fixture
    if alpha == 21:
        test = test.replace('update_data_with', 'update_with')
    if alpha == 28:
        test = test.replace('// STREAM_ID', 'let stream_id = rh.leader.progress.get(&3).stream_id;')
        test = test.replace('// STREAM_ARG', 'stream_id,')
        test = test.replace('// REMOVED_STREAM_ARG', 'crate::progress::stream_id::StreamId::new(41),')
        test = test.replace('rh.state.committed()', 'rh.state.cluster_committed()')
    target = source / 'src/engine/handler/replication_handler/update_matching_test.rs'
    with target.open('a') as out:
        out.write('\n' + test)
    # The published manifests use prerelease ranges. Pin the matching runtime
    # and macro APIs so this reproducer does not silently test newer releases.
    manifest = source / 'Cargo.toml'
    contents = manifest.read_text()
    for dependency in ('openraft-macros', 'openraft-rt', 'openraft-rt-tokio'):
        contents = re.sub(r'(\[(?:dev-)?dependencies\.' + dependency + r'\]\nversion = ")[^"]+', r'\g<1>=' + version, contents)
    manifest.write_text(contents)
    env = dict(os.environ, CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_BUILD_JOBS='4', CARGO_TARGET_DIR=str(root / 'target'))
    with (root / f'alpha{alpha}.log').open('w') as log:
        result = subprocess.run(['cargo', 'test', '--lib', 'ursula_', '--', '--nocapture'], cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
    output = (root / f'alpha{alpha}.log').read_text()
    expected = '4 failed' if alpha == 21 else '4 passed'
    if expected not in output or (alpha == 28 and result.returncode):
        raise SystemExit(f'Unexpected result for {version}; inspect {root / f"alpha{alpha}.log"}')
    print(f'{version}: expected {expected}; log={root / f"alpha{alpha}.log"}', flush=True)
