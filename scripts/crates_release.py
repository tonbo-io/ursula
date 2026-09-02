#!/usr/bin/env python3
"""Validate or publish every public Ursula workspace crate in dependency order."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from typing import Any


CRATES_IO = "https://crates.io/api/v1/crates"


def cargo_metadata() -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def publishable_packages(metadata: dict[str, Any], version: str) -> dict[str, dict[str, Any]]:
    workspace_members = set(metadata["workspace_members"])
    packages: dict[str, dict[str, Any]] = {}
    for package in metadata["packages"]:
        if package["id"] not in workspace_members:
            continue
        publish = package.get("publish")
        if publish == [] or (publish is not None and "crates-io" not in publish):
            continue
        if package["version"] != version:
            raise RuntimeError(
                f'{package["name"]} has version {package["version"]}, expected {version}'
            )
        packages[package["name"]] = package
    if not packages:
        raise RuntimeError("workspace contains no crates.io-publishable packages")
    return packages


def topological_order(packages: dict[str, dict[str, Any]]) -> list[str]:
    dependencies: dict[str, set[str]] = {}
    for name, package in packages.items():
        dependencies[name] = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in packages and dependency.get("kind") != "dev"
        }

    order: list[str] = []
    remaining = {name: set(required) for name, required in dependencies.items()}
    while remaining:
        ready = sorted(name for name, required in remaining.items() if not required)
        if not ready:
            cycle = ", ".join(
                f"{name} -> {sorted(required)}" for name, required in sorted(remaining.items())
            )
            raise RuntimeError(f"workspace publication dependency cycle: {cycle}")
        order.extend(ready)
        for name in ready:
            del remaining[name]
        for required in remaining.values():
            required.difference_update(ready)
    return order


def internal_dependencies(
    package: dict[str, Any], packages: dict[str, dict[str, Any]]
) -> set[str]:
    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["name"] in packages and dependency.get("kind") != "dev"
    }


def validate_internal_dependency_requirements(
    packages: dict[str, dict[str, Any]], version: str
) -> None:
    expected = f"={version}"
    for package_name, package in packages.items():
        for dependency in package["dependencies"]:
            if dependency["name"] not in packages or dependency.get("kind") == "dev":
                continue
            if dependency.get("req") != expected:
                raise RuntimeError(
                    f"{package_name} depends on {dependency['name']} with "
                    f"{dependency.get('req')!r}, expected {expected!r}"
                )


def crate_version_exists(name: str, version: str) -> bool:
    request = urllib.request.Request(
        f"{CRATES_IO}/{name}/{version}",
        headers={"User-Agent": "tonbo-ursula-release/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    returned = payload.get("version", {}).get("num")
    if returned != version:
        raise RuntimeError(f"crates.io returned unexpected version for {name}: {returned!r}")
    return True


def wait_for_index(name: str, version: str, timeout_seconds: int = 300) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if crate_version_exists(name, version):
            return True
        time.sleep(5)
    return False


def run(command: Iterable[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(list(command), check=True, env=env)


def validate(order: list[str], packages: dict[str, dict[str, Any]]) -> None:
    for name in order:
        print(f"checking package contents for {name}", flush=True)
        run(["cargo", "package", "--list", "--locked", "-p", name])
        if not internal_dependencies(packages[name], packages):
            print(f"checking publish archive for leaf crate {name}", flush=True)
            run(["cargo", "publish", "--dry-run", "--no-verify", "--locked", "-p", name])


def publish(order: list[str], version: str) -> None:
    token = os.environ.get("CARGO_REGISTRY_TOKEN", "")
    if not token:
        raise RuntimeError("CARGO_REGISTRY_TOKEN is required for publication")
    environment = os.environ.copy()

    for name in order:
        if crate_version_exists(name, version):
            print(f"{name} {version} already exists; retaining the immutable crate", flush=True)
            continue

        print(f"publishing {name} {version}", flush=True)
        try:
            run(["cargo", "publish", "--locked", "-p", name], env=environment)
        except subprocess.CalledProcessError:
            if crate_version_exists(name, version):
                print(f"{name} {version} reached crates.io despite the client error", flush=True)
            else:
                raise

        if not wait_for_index(name, version):
            raise RuntimeError(f"{name} {version} did not appear in the crates.io API")


def verify(order: list[str], version: str) -> None:
    missing = [name for name in order if not crate_version_exists(name, version)]
    if missing:
        raise RuntimeError(f"crates.io is missing {version} for: {', '.join(missing)}")
    print(f"verified {len(order)} crates.io packages at {version}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("check", "publish", "verify"))
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    packages = publishable_packages(cargo_metadata(), args.version)
    validate_internal_dependency_requirements(packages, args.version)
    order = topological_order(packages)
    print("publication order: " + ", ".join(order), flush=True)

    if args.mode == "check":
        validate(order, packages)
    elif args.mode == "publish":
        publish(order, args.version)
    else:
        verify(order, args.version)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        print(f"crates release failed: {error}", file=sys.stderr)
        sys.exit(1)
