#!/usr/bin/env bash
# Generates the backup-format-4 fixture at v0.8.2 (never merged).
set -euo pipefail
cargo test -p ursula --lib a0_09_generate_backup_format_4_fixture -- --ignored --nocapture
ls -la target/backup-format-4
