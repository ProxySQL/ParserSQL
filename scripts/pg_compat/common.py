import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit


REQUIRED_ROLES = ("previous", "target")
REQUIRED_VERSION_FIELDS = (
    "branch",
    "commit",
    "pg_version",
    "pg_version_num",
    "postgres_sha256",
)
COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
PG_VERSION_PATTERN = re.compile(r"^[0-9]+(?:\.[0-9]+)+$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _require_nonempty_string(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _validate_version_pin(version, role):
    prefix = f"versions.{role}"
    for field in REQUIRED_VERSION_FIELDS:
        if field not in version:
            raise ValueError(f"missing required pin: {prefix}.{field}")

    _require_nonempty_string(version["branch"], f"{prefix}.branch")
    _require_nonempty_string(version["pg_version"], f"{prefix}.pg_version")

    if not isinstance(version["commit"], str) or not COMMIT_PATTERN.fullmatch(
        version["commit"]
    ):
        raise ValueError(f"{prefix}.commit must be 40 lowercase hexadecimal characters")
    if not PG_VERSION_PATTERN.fullmatch(version["pg_version"]):
        raise ValueError(f"{prefix}.pg_version must use numeric dotted components")
    if isinstance(version["pg_version_num"], bool) or not isinstance(
        version["pg_version_num"], int
    ):
        raise ValueError(f"{prefix}.pg_version_num must be an integer")
    if not isinstance(
        version["postgres_sha256"], str
    ) or not SHA256_PATTERN.fullmatch(version["postgres_sha256"]):
        raise ValueError(
            f"{prefix}.postgres_sha256 must be 64 lowercase hexadecimal characters"
        )


def load_pins(path):
    path = Path(path)
    with path.open(encoding="utf-8") as pins_file:
        pins = json.load(pins_file)

    if not isinstance(pins, dict) or "libpg_query_url" not in pins:
        raise ValueError("missing required pin: libpg_query_url")
    _require_nonempty_string(pins["libpg_query_url"], "libpg_query_url")
    parsed_url = urlsplit(pins["libpg_query_url"])
    if not parsed_url.scheme or not parsed_url.netloc:
        raise ValueError("libpg_query_url must be a non-empty absolute URL")

    versions = pins.get("versions")
    if not isinstance(versions, dict):
        raise ValueError("missing required pin: versions")

    for role in REQUIRED_ROLES:
        version = versions.get(role)
        if not isinstance(version, dict):
            raise ValueError(f"missing required pin: versions.{role}")
        _validate_version_pin(version, role)

    return pins


def parse_makefile_pg_version(text):
    version_match = re.search(r"^PG_VERSION\s*=\s*(\S+)\s*$", text, re.MULTILINE)
    version_num_match = re.search(r"^PG_VERSION_NUM\s*=\s*(\d+)\s*$", text, re.MULTILINE)

    if version_match is None:
        raise ValueError("PG_VERSION is absent from Makefile")
    if version_num_match is None:
        raise ValueError("PG_VERSION_NUM is absent from Makefile")

    return version_match.group(1), int(version_num_match.group(1))


def statement_id(normalized_sql, oracle_node):
    digest_input = f"{oracle_node}\0{normalized_sql}".encode("utf-8")
    return hashlib.sha256(digest_input).hexdigest()[:24]


def read_jsonl(path):
    path = Path(path)
    with path.open(encoding="utf-8") as jsonl_file:
        for line_number, line in enumerate(jsonl_file, start=1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"{path}:{line_number}: invalid JSON: {error.msg}"
                ) from error


def atomic_write_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )

    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary_file:
            temporary_file.write(text)
        os.replace(temporary_path, path)
    except BaseException:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise
