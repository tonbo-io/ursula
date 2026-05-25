"""Ursula DST audit + reporting CLI.

Single entry point: `python3 -m scripts.dst <subcommand>`. See `cli.py` for
the subcommand list, or run `python3 -m scripts.dst --help`.

This package replaces the 11 standalone DST scripts that previously lived
directly under `scripts/`. Each subcommand here is a self-contained function;
shared parsing / corpus / workflow helpers live in `common.py`.
"""
