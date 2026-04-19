"""
S3 -> local sync helper for populating the local data lake mirror.

Notebooks use this to pull Bronze/Silver/Gold layers from S3 to disk on demand.
The shell scripts (01/02/03) are the source of truth for populating S3;
this module is the read-only inverse that mirrors a prefix locally.
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def _build_session(profile: Optional[str] = None) -> boto3.Session:
    profile = profile or os.getenv("AWS_PROFILE")
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()


def sync_s3_prefix_to_local(
    bucket: str,
    prefix: str,
    local_dir: Path,
    profile: Optional[str] = None,
    max_workers: int = 8,
) -> dict:
    """
    Mirror every object under ``s3://bucket/prefix`` into ``local_dir``.

    Idempotent: skips files that already exist locally with matching size.

    :param bucket: S3 bucket name.
    :param prefix: Key prefix (e.g. ``"bronze/"``). Trailing slash optional.
    :param local_dir: Local root directory that mirrors ``prefix``.
    :param profile: AWS profile name (falls back to ``AWS_PROFILE`` env var).
    :param max_workers: Concurrent downloads.
    :return: ``{"downloaded": N, "skipped": M, "errors": K}``.
    """
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    normalized_prefix = prefix if prefix.endswith("/") else prefix + "/"

    session = _build_session(profile)
    s3 = session.client("s3")

    # List every object under the prefix (paginated) in a single pass.
    to_download: list[tuple[str, Path, int]] = []
    skipped = 0
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                size: int = obj["Size"]

                if key.endswith("/"):
                    continue

                rel = key[len(normalized_prefix):]
                target = local_dir / rel

                if target.exists() and target.stat().st_size == size:
                    skipped += 1
                    continue

                to_download.append((key, target, size))
    except ClientError as exc:
        logger.error("S3 list failed for s3://%s/%s: %s", bucket, normalized_prefix, exc)
        return {"downloaded": 0, "skipped": 0, "errors": 1}

    if not to_download:
        logger.info("[sync] s3://%s/%s -> %s : already up to date (%d files)",
                    bucket, normalized_prefix, local_dir, skipped)
        return {"downloaded": 0, "skipped": skipped, "errors": 0}

    logger.info("[sync] s3://%s/%s -> %s : %d to download, %d up to date",
                bucket, normalized_prefix, local_dir, len(to_download), skipped)

    def _download(item: tuple[str, Path, int]) -> bool:
        key, target, _size = item
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3.download_file(bucket, key, str(target))
            return True
        except ClientError as exc:
            logger.error("Failed to download %s: %s", key, exc)
            return False

    downloaded = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="s3sync") as pool:
        futures = [pool.submit(_download, item) for item in to_download]
        for fut in as_completed(futures):
            if fut.result():
                downloaded += 1
            else:
                errors += 1

    logger.info("[sync] done: downloaded=%d skipped=%d errors=%d",
                downloaded, skipped, errors)
    return {"downloaded": downloaded, "skipped": skipped, "errors": errors}
