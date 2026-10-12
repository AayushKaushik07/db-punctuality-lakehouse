"""Stage 1 – download monthly Parquet files from Hugging Face into the landing zone.

Files are stored untouched. A small JSON manifest records size and SHA-256 of every
file so later stages can detect whether a month changed since it was last loaded.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from dbpipe.config import Settings
from dbpipe.logging_utils import get_logger

log = get_logger("download")

MANIFEST_NAME = "_manifest.json"


def remote_filename(month: str) -> str:
    return f"monthly_processed_data/data-{month}.parquet"


def landing_file(settings: Settings, month: str) -> Path:
    return settings.path("landing") / remote_filename(month)


def sha256_of(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(settings: Settings) -> dict:
    path = settings.path("landing") / MANIFEST_NAME
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def update_manifest(settings: Settings, month: str, source: str) -> dict:
    """(Re)compute the manifest entry for one month from the file on disk."""
    file_path = landing_file(settings, month)
    manifest = read_manifest(settings)
    entry = {
        "file": str(file_path.relative_to(settings.path("landing"))),
        "bytes": file_path.stat().st_size,
        "sha256": sha256_of(file_path),
        "registered_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": source,
    }
    manifest[month] = entry
    manifest_path = settings.path("landing") / MANIFEST_NAME
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return entry


def download_months(settings: Settings, force: bool = False, retries: int = 3) -> None:
    from huggingface_hub import hf_hub_download  # imported lazily (not needed offline)

    landing = settings.path("landing")
    landing.mkdir(parents=True, exist_ok=True)
    manifest = read_manifest(settings)

    for month in settings.months:
        target = landing_file(settings, month)
        if target.exists() and month in manifest and not force:
            log.info("%s already in landing zone (%.1f MB) – skipping", month, target.stat().st_size / 1e6)
            continue

        for attempt in range(1, retries + 1):
            try:
                log.info("Downloading %s from %s (attempt %d)…", month, settings.hf_repo_id, attempt)
                started = time.time()
                hf_hub_download(
                    repo_id=settings.hf_repo_id,
                    filename=remote_filename(month),
                    repo_type="dataset",
                    local_dir=str(landing),
                    force_download=force,
                )
                log.info("  done in %.0fs", time.time() - started)
                break
            except Exception as exc:  # network errors, 5xx, ...
                if attempt == retries:
                    raise RuntimeError(
                        f"Could not download {month}. Does the month exist in the dataset? "
                        f"See https://huggingface.co/datasets/{settings.hf_repo_id}"
                    ) from exc
                wait = 10 * attempt
                log.warning("  failed (%s) – retrying in %ds", exc, wait)
                time.sleep(wait)

        entry = update_manifest(settings, month, source=f"hf://{settings.hf_repo_id}")
        log.info("  %s: %.1f MB, sha256=%s…", month, entry["bytes"] / 1e6, entry["sha256"][:12])
