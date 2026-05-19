set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

build:
    cargo build --workspace

test:
    cargo test --workspace

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

ec2-status config:
    python3 scripts/ursula_ec2.py --config {{config}} status

ec2-upload-server config binary:
    python3 scripts/ursula_ec2.py --config {{config}} upload-binary --target servers --local {{binary}} --remote /tmp/ursula-http

ec2-upload-client config binary:
    python3 scripts/ursula_ec2.py --config {{config}} upload-binary --target client --local {{binary}} --remote /tmp/perf_compare

ec2-perf-many config processes bucket_prefix:
    python3 scripts/ursula_ec2.py --config {{config}} perf-many --processes {{processes}} --bucket-prefix {{bucket_prefix}} -- --phases write,small,mixed,read --concurrency 256 --throughput-secs 30 --payload-bytes 128 --small-payload-bytes 128 --ursula-append-mode batch --ursula-append-batch-size 16 --ursula-append-batch-minimal-ack --mixed-appenders 128 --mixed-readers 64 --mixed-sse-readers 8 --sse-count 100 --request-timeout-secs 60 --setup-concurrency 256 --validate-read-len

ec2-start config:
    python3 scripts/ursula_ec2.py --config {{config}} start
    python3 scripts/ursula_ec2.py --config {{config}} wait-ready

ec2-stop config:
    python3 scripts/ursula_ec2.py --config {{config}} stop
