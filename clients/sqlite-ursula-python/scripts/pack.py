"""Packs and checks the wheels of a release (.github/workflows/sqlite-ursula-python-wheels.yml).

    python scripts/pack.py pack <extensions> <out> <version>
    python scripts/pack.py check <out>

``pack`` builds, with hatchling through ``uv build``, one wheel per entry of ``PLATFORMS``, carrying
``<extensions>/<platform>/<file>`` (the artifacts of sqlite-vfs-build.yml) under
``sqlite_ursula/_lib/``, and retags it ``py3-none-<tag>`` with ``wheel tags``: the library links no
libpython, so one wheel per platform serves every CPython 3.11 or newer, free-threaded builds
included. A ``py3-none-any`` wheel without a library lets the package install anywhere, and there
``open()`` raises ``host_unsupported`` unless ``SQLITE_URSULA_VFS`` names a library. ``<version>``
is the package's own version, or a ``.devN`` pre-release, which the wheels carry as
``__version__``. Every wheel must hold exactly the files it should.

The workflow then grafts libgcc_s into the musl wheel with ``auditwheel repair``: the library links
it for unwinding, musllinux allows none from the host, and ``python:3.13-alpine`` has none.

``check`` runs ``auditwheel show`` on each Linux wheel, which must find the library consistent with
the wheel's tag and needing no library outside the tag's policy, checks that only the musl wheel
carries a grafted library, libgcc_s, and runs ``twine check --strict`` on every wheel.

It needs uv on ``PATH`` and only the standard library.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INIT = Path("src") / "sqlite_ursula" / "__init__.py"
VERSION_LINE = re.compile(r'^__version__ = "([^"]*)"$', re.MULTILINE)

#: The platform name of sqlite-vfs-build.yml, the library's file, and the wheel's platform tag.
PLATFORMS = [
    ("linux-x64-gnu", "libsqlite_ursula_vfs.so", "manylinux_2_28_x86_64"),
    ("linux-arm64-gnu", "libsqlite_ursula_vfs.so", "manylinux_2_28_aarch64"),
    ("linux-x64-musl", "libsqlite_ursula_vfs.so", "musllinux_1_2_x86_64"),
    ("darwin-arm64", "libsqlite_ursula_vfs.dylib", "macosx_11_0_arm64"),
]

#: The one library auditwheel grafts, into the musl wheel.
GRAFTED = r"sqlite_ursula\.libs/libgcc_s-[0-9a-f]+\.so\.1"

AUDITWHEEL = "auditwheel==6.8.2"
TWINE = "twine==7.0.0"
WHEEL = "wheel==0.48.0"


def run(*args: str | Path) -> str:
    print("+", " ".join(str(a) for a in args), file=sys.stderr, flush=True)
    return subprocess.run(
        [str(a) for a in args], check=True, stdout=subprocess.PIPE, text=True
    ).stdout


def own_version() -> str:
    found = VERSION_LINE.findall((ROOT / INIT).read_text())
    if len(found) != 1:
        raise SystemExit(f"{INIT} has {len(found)} __version__ lines, not one")
    return found[0]


def check_version(version: str) -> None:
    """The package's own version, or a ``.devN`` pre-release of a later ``X.Y.Z``."""
    own = own_version()
    if version == own:
        return
    dev = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)\.dev(0|[1-9]\d*)", version)
    base = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", own)
    if dev is None or base is None:
        raise SystemExit(f"version {version} is neither {own} nor X.Y.Z.devN")
    if tuple(map(int, dev.groups()[:3])) <= tuple(map(int, base.groups())):
        raise SystemExit(f"version {version} is a pre-release of a version not after {own}")


def project(version: str) -> Path:
    """A copy of the project to build, with ``version`` as ``__version__``."""
    copy = Path(tempfile.mkdtemp(prefix="sqlite-ursula-project-"))
    for name in ("pyproject.toml", "README.md", "LICENSE", ".gitignore"):
        shutil.copy2(ROOT / name, copy / name)
    shutil.copytree(
        ROOT / "src", copy / "src", ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "_lib")
    )
    init = copy / INIT
    init.write_text(VERSION_LINE.sub(f'__version__ = "{version}"', init.read_text()))
    return copy


def build(source: Path, out: Path) -> Path:
    """The pure wheel hatchling builds from ``source``, in ``out``."""
    with tempfile.TemporaryDirectory(prefix="sqlite-ursula-wheel-") as tmp:
        run("uv", "build", "--wheel", "--out-dir", tmp, source)
        (wheel,) = Path(tmp).glob("*.whl")
        return Path(shutil.move(wheel, out / wheel.name))


def names(wheel: Path) -> set[str]:
    with zipfile.ZipFile(wheel) as z:
        return set(z.namelist())


def wheel_tag(wheel: Path) -> list[str]:
    with zipfile.ZipFile(wheel) as z:
        (info,) = [n for n in z.namelist() if n.endswith(".dist-info/WHEEL")]
        text = z.read(info).decode()
    return [line.removeprefix("Tag: ") for line in text.splitlines() if line.startswith("Tag: ")]


def wheel_version(wheel: Path) -> str:
    with zipfile.ZipFile(wheel) as z:
        (meta,) = [n for n in z.namelist() if n.endswith(".dist-info/METADATA")]
        text = z.read(meta).decode()
    (version,) = re.findall(r"^Version: (.*)$", text, re.MULTILINE)
    return version


def pack(extensions: Path, out: Path, version: str) -> None:
    check_version(version)
    out.mkdir(parents=True, exist_ok=True)
    source = project(version)
    lib = source / "src" / "sqlite_ursula" / "_lib"

    fallback = build(source, out)
    if not fallback.name.endswith("-py3-none-any.whl"):
        raise SystemExit(f"hatchling built {fallback.name}, not a py3-none-any wheel")
    base = names(fallback)
    if any("/_lib/" in n for n in base):
        raise SystemExit(f"{fallback.name} carries a library")
    if wheel_version(fallback) != version:
        raise SystemExit(f"{fallback.name} is version {wheel_version(fallback)}, not {version}")
    built = [fallback]

    for platform, file, tag in PLATFORMS:
        shutil.rmtree(lib, ignore_errors=True)
        lib.mkdir()
        shutil.copy2(extensions / platform / file, lib / file)
        with tempfile.TemporaryDirectory(prefix="sqlite-ursula-tags-") as tmp:
            pure = build(source, Path(tmp))
            run("uvx", "--from", WHEEL, "wheel", "tags", "--remove", "--platform-tag", tag, pure)
            (wheel,) = Path(tmp).glob("*.whl")
            wheel = Path(shutil.move(wheel, out / wheel.name))
        if not wheel.name.endswith(f"-py3-none-{tag}.whl") or wheel_tag(wheel) != [
            f"py3-none-{tag}"
        ]:
            raise SystemExit(f"{wheel.name} is tagged {wheel_tag(wheel)}, not py3-none-{tag}")
        extra = names(wheel) - base
        missing = base - names(wheel)
        # RECORD and WHEEL differ in content, not in name.
        if extra != {f"sqlite_ursula/_lib/{file}"} or missing:
            raise SystemExit(f"{wheel.name}: adds {sorted(extra)}, lacks {sorted(missing)}")
        built.append(wheel)
    shutil.rmtree(source)

    print("\n".join(w.name for w in built))


def policy(tag: str) -> tuple[str, tuple[int, int], str]:
    """``manylinux_2_28_x86_64`` as ``("manylinux", (2, 28), "x86_64")``."""
    found = re.fullmatch(r"(manylinux|musllinux)_(\d+)_(\d+)_(\w+)", tag)
    if found is None:
        raise SystemExit(f"{tag} is not a PEP 600 or PEP 656 tag")
    family, major, minor, arch = found.groups()
    return family, (int(major), int(minor)), arch


def check(out: Path) -> None:
    wheels = sorted(out.glob("*.whl"))
    linux = [w for w in wheels if "linux_" in w.name]
    if len(wheels) != len(PLATFORMS) + 1 or len(linux) != 3:
        raise SystemExit(f"{out} holds {[w.name for w in wheels]}")
    for wheel in linux:
        (tag,) = wheel_tag(wheel)
        family, floor, arch = policy(tag.removeprefix("py3-none-"))
        report = json.loads(run("uvx", AUDITWHEEL, "show", "--json", wheel))
        print(json.dumps(report, indent=2))
        found = policy(report["overall_tag"])
        if found[0] != family or found[2] != arch or found[1] > floor:
            raise SystemExit(
                f"{wheel.name}: auditwheel finds it {report['overall_tag']}, not within {tag}"
            )
        if report["external_libs"] or report["unsupported_isa"]:
            raise SystemExit(f"{wheel.name}: needs libraries outside its policy, or ISA extensions")
    for wheel in wheels:
        grafted = sorted(n for n in names(wheel) if n.startswith("sqlite_ursula.libs/"))
        if "-musllinux_" in wheel.name:
            ok = len(grafted) == 1 and re.fullmatch(GRAFTED, grafted[0]) is not None
        else:
            ok = not grafted
        if not ok:
            raise SystemExit(f"{wheel.name} carries the grafted libraries {grafted}")
    run("uvx", TWINE, "check", "--strict", *wheels)


def main() -> None:
    match sys.argv[1:]:
        case ["pack", extensions, out, version]:
            pack(Path(extensions).resolve(), Path(out).resolve(), version)
        case ["check", out]:
            check(Path(out).resolve())
        case _:
            raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
