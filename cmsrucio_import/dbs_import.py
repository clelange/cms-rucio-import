"""Import legacy DBS datasets into a CMS Rucio user scope.

The importer intentionally stages files on the source site's temporary RSE.
Rucio then creates the managed replica through a replication rule.  Users must
not write directly below ``/store/user/rucio`` on a regular CMS RSE.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

LOGGER = logging.getLogger("dbs-dataset-import")
_DATASET_RE = re.compile(r"^/[^/]+/[^/]+/[^/]+$")
_ADLER32_RE = re.compile(r"^[0-9a-fA-F]{1,8}$")
_LEGACY_LFN_ROOT_RE = re.compile(r"^/store/(?:user|group)/[^/]+/")


class ImportConfigurationError(ValueError):
    """The import configuration or source metadata is inconsistent."""


class DBSRequestError(RuntimeError):
    """A DBSReader request failed."""


class ImportPreflightError(RuntimeError):
    """The import cannot safely start."""


class MetadataConflictError(RuntimeError):
    """A target Rucio DID exists with different immutable metadata."""


class TransferError(RuntimeError):
    """A source validation or temporary copy failed."""


def _chunks(values: Sequence[Any], size: int = 500) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _normalise_adler32(value: Any) -> str:
    checksum = str(value or "").lower().removeprefix("0x")
    if not _ADLER32_RE.fullmatch(checksum):
        raise ImportConfigurationError(f"Invalid Adler-32 checksum: {value!r}")
    return checksum.zfill(8)


def _did_type_name(value: Any) -> str:
    if hasattr(value, "name"):
        value = value.name
    return str(value).upper().split(".")[-1]


def _exception_name(error: BaseException) -> str:
    return type(error).__name__


@dataclass(frozen=True)
class DBSBlock:
    name: str
    origin_site: str | None
    file_count: int
    size: int


@dataclass(frozen=True)
class DBSFile:
    source_lfn: str
    block: str
    size: int
    adler32: str
    valid: bool
    event_count: int | None = None


@dataclass(frozen=True)
class ManifestBlock:
    name: str
    origin_site: str | None
    file_count: int
    size: int


@dataclass(frozen=True)
class ManifestFile:
    source_lfn: str
    target_lfn: str
    block: str
    size: int
    adler32: str
    valid: bool
    source_pfn: str
    temp_pfn: str
    event_count: int | None = None


@dataclass(frozen=True)
class ImportManifest:
    version: int
    created_at: str
    account: str
    scope: str
    dataset: str
    dbs_instance: str
    include_invalid_files: bool
    container: str
    source_rse: str
    temp_rse: str
    target_rse: str
    source_lfn_prefix: str
    temp_lfn_prefix: str
    target_lfn_prefix: str
    copies: int
    lifetime: int | None
    blocks: tuple[ManifestBlock, ...]
    files: tuple[ManifestFile, ...]

    @property
    def total_bytes(self) -> int:
        return sum(item.size for item in self.files)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["totals"] = {
            "blocks": len(self.blocks),
            "files": len(self.files),
            "bytes": self.total_bytes,
        }
        return payload

    def write(self, destination: str | os.PathLike[str]) -> None:
        path = Path(destination)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n")


@dataclass(frozen=True)
class ImportConfig:
    dataset: str
    dbs_instance: str
    include_invalid_files: bool
    rucio_scope: str | None
    source_rse: str | None
    temp_rse: str | None
    target_rse: str | None
    temp_lfn_prefix: str | None
    lfn_from: str | None
    lfn_to: str | None
    container: str
    copies: int
    lifetime: int | None
    dry_run: bool
    manifest_path: str | None
    preflight_files: int

    @classmethod
    def from_specs(cls, specs: Mapping[str, Any]) -> ImportConfig:
        dataset = str(specs["dataset"])
        if not _DATASET_RE.fullmatch(dataset):
            raise ImportConfigurationError(
                f"Dataset must have CMS /primary/processed/tier form: {dataset}"
            )

        source = specs.get("source", {})
        destination = specs.get("destination", {})
        rewrite = specs.get("lfnRewrite", {})
        collection = specs.get("collection", {})
        rule = specs.get("rule", {})
        options = specs.get("options", {})

        copies = int(rule.get("copies", 1))
        raw_lifetime = rule.get("lifetime")
        lifetime = int(raw_lifetime) if raw_lifetime is not None else None
        preflight_files = int(options.get("preflightFiles", 0))
        if copies != 1:
            raise ImportConfigurationError(
                "This importer currently requires copies: 1 for an exact target RSE"
            )
        if lifetime is not None and lifetime <= 0:
            raise ImportConfigurationError(
                "Rule lifetime must be a positive number of seconds"
            )
        if preflight_files < 0:
            raise ImportConfigurationError("preflightFiles cannot be negative")

        lfn_from = str(rewrite["from"]) if rewrite.get("from") else None
        lfn_to = str(rewrite["to"]) if rewrite.get("to") else None
        temp_lfn_prefix = (
            str(destination["tempLFNPrefix"])
            if destination.get("tempLFNPrefix")
            else None
        )
        for prefix in (lfn_from, lfn_to, temp_lfn_prefix):
            if prefix is not None and not prefix.endswith("/"):
                raise ImportConfigurationError("LFN prefixes must end in '/'")
        if temp_lfn_prefix and not temp_lfn_prefix.startswith("/store/temp/user/"):
            raise ImportConfigurationError(
                "destination.tempLFNPrefix must use the submitting user's "
                f"/store/temp/user namespace, got {temp_lfn_prefix}"
            )

        return cls(
            dataset=dataset,
            dbs_instance=str(specs.get("dbsInstance", "phys03")),
            include_invalid_files=bool(specs.get("includeInvalidFiles", False)),
            rucio_scope=(
                str(specs["rucioScope"]) if specs.get("rucioScope") else None
            ),
            source_rse=str(source["rse"]) if source.get("rse") else None,
            temp_rse=(
                str(destination["tempRSE"])
                if destination.get("tempRSE")
                else None
            ),
            target_rse=(
                str(destination["rse"]) if destination.get("rse") else None
            ),
            temp_lfn_prefix=temp_lfn_prefix,
            lfn_from=lfn_from,
            lfn_to=lfn_to,
            container=str(collection.get("containerName", dataset)),
            copies=copies,
            lifetime=lifetime,
            dry_run=bool(options.get("dryRun", True)),
            manifest_path=(
                str(options["manifestPath"]) if options.get("manifestPath") else None
            ),
            preflight_files=preflight_files,
        )


class DBSReader:
    """Small authenticated client for the DBSReader endpoints used here."""

    def __init__(
        self,
        instance: str = "phys03",
        *,
        session: requests.Session | None = None,
        proxy_path: str | None = None,
        verify: str | bool | None = None,
        timeout: int = 180,
    ) -> None:
        self.instance = instance
        self.base_url = self._base_url(instance)
        self.session = session or requests.Session()
        self.proxy_path = proxy_path or self._default_proxy_path()
        self.verify = self._default_verify() if verify is None else verify
        self.timeout = timeout

    @staticmethod
    def _base_url(instance: str) -> str:
        if instance.startswith("https://"):
            return instance.rstrip("/")
        short_name = instance.removeprefix("prod/")
        if short_name not in {"global", "phys01", "phys02", "phys03"}:
            raise ImportConfigurationError(f"Unsupported DBS instance: {instance}")
        return f"https://cmsweb.cern.ch/dbs/prod/{short_name}/DBSReader"

    @staticmethod
    def _default_proxy_path() -> str | None:
        configured = os.environ.get("X509_USER_PROXY")
        if configured:
            return configured
        candidate = f"/tmp/x509up_u{os.getuid()}"
        return candidate if os.path.exists(candidate) else None

    @staticmethod
    def _default_verify() -> str | bool:
        for variable in ("REQUESTS_CA_BUNDLE", "X509_CERT_DIR"):
            candidate = os.environ.get(variable)
            if candidate and os.path.exists(candidate):
                return candidate
        return True

    def _get(self, endpoint: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        request_options: dict[str, Any] = {
            "params": params,
            "timeout": self.timeout,
            "verify": self.verify,
        }
        if self.proxy_path:
            request_options["cert"] = (self.proxy_path, self.proxy_path)
        try:
            response = self.session.get(
                f"{self.base_url}/{endpoint}", **request_options
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as error:
            raise DBSRequestError(
                f"DBSReader {endpoint} request failed for {params}: {error}"
            ) from error
        if not isinstance(payload, list):
            raise DBSRequestError(
                f"DBSReader {endpoint} returned {type(payload).__name__}, expected a list"
            )
        return payload

    def list_blocks(self, dataset: str) -> list[DBSBlock]:
        records = self._get("blocks", {"dataset": dataset, "detail": "true"})
        blocks = []
        for record in records:
            blocks.append(
                DBSBlock(
                    name=str(record["block_name"]),
                    origin_site=(
                        str(record["origin_site_name"])
                        if record.get("origin_site_name")
                        else None
                    ),
                    file_count=int(record.get("file_count") or 0),
                    size=int(record.get("block_size") or record.get("size") or 0),
                )
            )
        return blocks

    def list_files(self, dataset: str, include_invalid: bool = False) -> list[DBSFile]:
        records = self._get(
            "files",
            {
                "dataset": dataset,
                "detail": "true",
                "validFileOnly": 0 if include_invalid else 1,
            },
        )
        files = []
        for record in records:
            files.append(
                DBSFile(
                    source_lfn=str(record["logical_file_name"]),
                    block=str(record["block_name"]),
                    size=int(record["file_size"]),
                    adler32=_normalise_adler32(record["adler32"]),
                    valid=bool(int(record.get("is_file_valid", 1))),
                    event_count=(
                        int(record["event_count"])
                        if record.get("event_count") is not None
                        else None
                    ),
                )
            )
        return files


def _resolve_pfns(
    rucio_client: Any,
    rse: str,
    scope: str,
    lfns: Sequence[str],
    operation: str,
) -> dict[str, str]:
    if not lfns:
        return {}
    # CMS TFC mappings normally prepend one storage-specific prefix to the
    # complete LFN.  Resolve one name first and reuse that prefix.  Besides
    # being substantially faster for large CRAB datasets, this avoids the
    # HTTP 414 response produced by very long lfns2pfns query URLs.
    first_lfn = lfns[0]
    first_did = f"{scope}:{first_lfn}"
    first_result = rucio_client.lfns2pfns(rse, [first_did], operation=operation)
    first_pfn = first_result.get(first_did)
    if not first_pfn:
        raise ImportPreflightError(f"Rucio did not resolve {first_did} at {rse}")
    if first_pfn.endswith(first_lfn):
        prefix = first_pfn[: -len(first_lfn)]
        return {lfn: prefix + lfn for lfn in lfns}

    resolved: dict[str, str] = {}
    for batch in _chunks(lfns, 10):
        dids = [f"{scope}:{lfn}" for lfn in batch]
        result = rucio_client.lfns2pfns(rse, dids, operation=operation)
        for lfn in batch:
            key = f"{scope}:{lfn}"
            if key not in result:
                raise ImportPreflightError(f"Rucio did not resolve {key} at {rse}")
            resolved[lfn] = result[key]
    return resolved


def _resolve_account_scope(config: ImportConfig, rucio_client: Any) -> str:
    account = str(rucio_client.account)
    try:
        scopes = sorted(set(rucio_client.list_scopes_for_account(account)))
    except Exception as error:
        raise ImportPreflightError(
            f"Could not list scopes owned by Rucio account {account}: {error}"
        ) from error

    if config.rucio_scope:
        if config.rucio_scope not in scopes:
            raise ImportConfigurationError(
                f"Rucio account {account} does not own configured scope "
                f"{config.rucio_scope}; available scopes: {scopes}"
            )
        scope = config.rucio_scope
    elif len(scopes) == 1:
        scope = scopes[0]
    else:
        raise ImportConfigurationError(
            "rucioScope is required unless the authenticated account owns exactly "
            f"one scope; account {account} owns {scopes}"
        )

    namespace = scope.partition(".")[0]
    if namespace not in {"user", "group"}:
        raise ImportConfigurationError(
            f"Scope {scope} is not a CMS user or group scope"
        )
    return scope


def _infer_source_lfn_prefix(files: Sequence[DBSFile]) -> str:
    """Infer the legacy owner root shared by user- or group-produced files."""

    prefixes: set[str] = set()
    unsupported: list[str] = []
    for item in files:
        match = _LEGACY_LFN_ROOT_RE.match(item.source_lfn)
        if match:
            prefixes.add(match.group(0))
        else:
            unsupported.append(item.source_lfn)
    if unsupported:
        raise ImportPreflightError(
            "Could not infer the legacy LFN root from /store/user/<owner>/ or "
            "/store/group/<group>/; set lfnRewrite.from explicitly. First "
            f"unsupported LFN: {unsupported[0]}"
        )
    if len(prefixes) != 1:
        raise ImportPreflightError(
            "lfnRewrite.from is required when files do not share one legacy "
            f"user/group root; found {sorted(prefixes)}"
        )
    return next(iter(prefixes))


def build_manifest(
    config: ImportConfig,
    dbs_reader: DBSReader,
    rucio_client: Any,
) -> ImportManifest:
    """Resolve a DBS dataset into an immutable, auditable import manifest."""

    blocks = dbs_reader.list_blocks(config.dataset)
    files = dbs_reader.list_files(config.dataset, config.include_invalid_files)
    if not blocks:
        raise ImportPreflightError(f"DBS returned no blocks for {config.dataset}")
    if not files:
        qualifier = (
            "including invalid files" if config.include_invalid_files else "valid"
        )
        raise ImportPreflightError(
            f"DBS returned no {qualifier} files for {config.dataset}"
        )

    block_by_name = {block.name: block for block in blocks}
    unknown_blocks = sorted({item.block for item in files} - set(block_by_name))
    if unknown_blocks:
        raise ImportPreflightError(
            f"DBS files refer to blocks missing from the block query: {unknown_blocks[:3]}"
        )

    origins = {block_by_name[item.block].origin_site for item in files}
    origins.discard(None)
    if config.source_rse:
        source_rse = config.source_rse
    elif len(origins) == 1:
        source_rse = next(iter(origins))
    else:
        raise ImportPreflightError(
            "source.rse is required when DBS blocks do not have one common origin site; "
            f"found {sorted(origins)}"
        )

    expected_temp_rse = f"{source_rse}_Temp"
    temp_rse = config.temp_rse or expected_temp_rse
    target_rse = config.target_rse or source_rse
    if temp_rse != expected_temp_rse:
        raise ImportConfigurationError(
            "Same-storage imports require destination.tempRSE to be the source RSE's "
            f"temporary RSE ({expected_temp_rse}), got {temp_rse}"
        )

    rse_info: dict[str, Mapping[str, Any]] = {}
    for rse in dict.fromkeys((source_rse, temp_rse, target_rse)):
        try:
            rse_info[rse] = rucio_client.get_rse(rse)
        except Exception as error:
            raise ImportPreflightError(
                f"RSE {rse} is not available: {error}"
            ) from error
    if not rse_info[source_rse].get("deterministic"):
        raise ImportPreflightError(f"Source RSE {source_rse} must be deterministic")
    if rse_info[temp_rse].get("deterministic"):
        raise ImportPreflightError(
            f"Temporary RSE {temp_rse} must be non-deterministic"
        )
    if not rse_info[target_rse].get("deterministic"):
        raise ImportPreflightError(
            f"Target RSE {target_rse} must be deterministic"
        )
    if rse_info[source_rse].get("availability_read") is False:
        raise ImportPreflightError(f"Source RSE {source_rse} is not readable")
    for writable_rse in (temp_rse, target_rse):
        if rse_info[writable_rse].get("availability_write") is False:
            raise ImportPreflightError(f"RSE {writable_rse} is not writable")

    account = str(rucio_client.account)
    scope = _resolve_account_scope(config, rucio_client)
    namespace = scope.partition(".")[0]
    expected_target_prefix = f"/store/{namespace}/rucio/{account}/"
    target_lfn_prefix = config.lfn_to or expected_target_prefix
    if target_lfn_prefix != expected_target_prefix:
        raise ImportConfigurationError(
            f"lfnRewrite.to must be the account's managed namespace "
            f"{expected_target_prefix}, got {target_lfn_prefix}"
        )
    source_lfn_prefix = config.lfn_from or _infer_source_lfn_prefix(files)
    if config.temp_lfn_prefix:
        temp_lfn_prefix = config.temp_lfn_prefix
    elif namespace == "user":
        temp_lfn_prefix = f"/store/temp/user/rucio/{account}/"
    else:
        raise ImportConfigurationError(
            "destination.tempLFNPrefix is required for group imports because "
            "CMS stages group output through the submitting user's "
            "/store/temp/user/<username>.<DN-hash>/ namespace"
        )

    target_lfns: dict[str, str] = {}
    temp_lfns: dict[str, str] = {}
    seen_targets: set[str] = set()
    seen_temps: set[str] = set()
    for item in files:
        if not item.source_lfn.startswith(source_lfn_prefix):
            raise ImportPreflightError(
                "Source LFN does not start with the inferred/configured legacy "
                f"root {source_lfn_prefix}: {item.source_lfn}"
            )
        target_lfn = (
            target_lfn_prefix + item.source_lfn[len(source_lfn_prefix) :]
        )
        if target_lfn in seen_targets:
            raise ImportPreflightError(f"LFN rewrite collision: {target_lfn}")
        relative_lfn = item.source_lfn[len(source_lfn_prefix) :]
        temp_lfn = temp_lfn_prefix + relative_lfn
        if temp_lfn in seen_temps:
            raise ImportPreflightError(f"Temporary LFN collision: {temp_lfn}")
        seen_targets.add(target_lfn)
        seen_temps.add(temp_lfn)
        target_lfns[item.source_lfn] = target_lfn
        temp_lfns[item.source_lfn] = temp_lfn

    source_lfns = [item.source_lfn for item in files]
    ordered_temp_lfns = [temp_lfns[item.source_lfn] for item in files]
    source_pfns = _resolve_pfns(rucio_client, source_rse, scope, source_lfns, "read")
    # The temporary RSE is non-deterministic.  Resolve its physical path with
    # the paired regular RSE's CMS TFC, exactly as CRAB/manual upload does.
    temp_pfns = _resolve_pfns(
        rucio_client, source_rse, scope, ordered_temp_lfns, "write"
    )

    manifest_files = []
    per_block: dict[str, list[DBSFile]] = {}
    for item, temp_name in zip(files, ordered_temp_lfns):
        source_pfn = source_pfns[item.source_lfn]
        temp_pfn = temp_pfns[temp_name]
        source_url = urlparse(source_pfn)
        temp_url = urlparse(temp_pfn)
        if (source_url.hostname, source_url.port) != (temp_url.hostname, temp_url.port):
            raise ImportPreflightError(
                "Source and temporary PFNs are not on the same storage endpoint: "
                f"{source_pfn} -> {temp_pfn}"
            )
        manifest_files.append(
            ManifestFile(
                source_lfn=item.source_lfn,
                target_lfn=target_lfns[item.source_lfn],
                block=item.block,
                size=item.size,
                adler32=item.adler32,
                valid=item.valid,
                source_pfn=source_pfn,
                temp_pfn=temp_pfn,
                event_count=item.event_count,
            )
        )
        per_block.setdefault(item.block, []).append(item)

    manifest_blocks = []
    for block_name in sorted(per_block):
        source_block = block_by_name[block_name]
        imported_files = per_block[block_name]
        manifest_blocks.append(
            ManifestBlock(
                name=block_name,
                origin_site=source_block.origin_site,
                file_count=len(imported_files),
                size=sum(item.size for item in imported_files),
            )
        )

    return ImportManifest(
        version=3,
        created_at=datetime.now(timezone.utc).isoformat(),
        account=account,
        scope=scope,
        dataset=config.dataset,
        dbs_instance=config.dbs_instance,
        include_invalid_files=config.include_invalid_files,
        container=config.container,
        source_rse=source_rse,
        temp_rse=temp_rse,
        target_rse=target_rse,
        source_lfn_prefix=source_lfn_prefix,
        temp_lfn_prefix=temp_lfn_prefix,
        target_lfn_prefix=target_lfn_prefix,
        copies=config.copies,
        lifetime=config.lifetime,
        blocks=tuple(manifest_blocks),
        files=tuple(
            sorted(manifest_files, key=lambda item: (item.block, item.source_lfn))
        ),
    )


@dataclass(frozen=True)
class QuotaStatus:
    available: bool
    source: str | None
    limit: int | None
    used: int
    required: int
    reason: str


def _limit_value(raw_limit: Any) -> int | None:
    if isinstance(raw_limit, Mapping):
        for key in ("bytes", "limit", "bytes_limit"):
            if raw_limit.get(key) is not None:
                return int(raw_limit[key])
        return None
    return int(raw_limit) if raw_limit is not None else None


def check_quota(rucio_client: Any, manifest: ImportManifest) -> QuotaStatus:
    """Check local and global account limits covering the exact target RSE."""

    account = manifest.account
    rse = manifest.target_rse
    required = manifest.total_bytes
    local_limits = rucio_client.get_local_account_limits(account)
    if rse in local_limits:
        limit = _limit_value(local_limits[rse])
        usage_records = list(rucio_client.get_local_account_usage(account, rse=rse))
        used = int(usage_records[0].get("bytes", 0)) if usage_records else 0
        available = limit == -1 or (limit is not None and limit - used >= required)
        return QuotaStatus(
            available=available,
            source=f"local:{rse}",
            limit=limit,
            used=used,
            required=required,
            reason="quota available" if available else "local quota is insufficient",
        )

    candidates: list[QuotaStatus] = []
    for expression, raw_limit in rucio_client.get_global_account_limits(
        account
    ).items():
        matching_rses = {item["rse"] for item in rucio_client.list_rses(expression)}
        if rse not in matching_rses:
            continue
        limit = _limit_value(raw_limit)
        usage_records = list(
            rucio_client.get_global_account_usage(account, rse_expression=expression)
        )
        used = int(usage_records[0].get("bytes", 0)) if usage_records else 0
        available = limit == -1 or (limit is not None and limit - used >= required)
        candidates.append(
            QuotaStatus(
                available=available,
                source=f"global:{expression}",
                limit=limit,
                used=used,
                required=required,
                reason="quota available"
                if available
                else "global quota is insufficient",
            )
        )

    for candidate in candidates:
        if candidate.available:
            return candidate
    if candidates:
        return max(
            candidates,
            key=lambda item: (
                float("inf") if item.limit == -1 else (item.limit or 0) - item.used
            ),
        )
    return QuotaStatus(
        available=False,
        source=None,
        limit=None,
        used=0,
        required=required,
        reason=f"account {account} has no quota covering {rse}",
    )


class GfalTransfer:
    """Checksum-validating same-storage copy operations using gfal2-util."""

    def __init__(self, *, timeout: int = 1800) -> None:
        self.timeout = timeout
        for command in ("gfal-copy", "gfal-stat", "gfal-sum"):
            if not shutil.which(command):
                raise ImportPreflightError(
                    f"{command} is required; run the importer on lxplus or a CMS UI host"
                )

    def _run(
        self, command: Sequence[str], *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                list(command),
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired as error:
            raise TransferError(
                f"{' '.join(command[:2])} timed out after {self.timeout} seconds"
            ) from error
        if check and result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise TransferError(f"{' '.join(command[:2])} failed: {detail}")
        return result

    def stat_size(self, pfn: str) -> int | None:
        result = self._run(("gfal-stat", pfn), check=False)
        if result.returncode:
            return None
        match = re.search(r"^\s*Size:\s*(\d+)", result.stdout, re.MULTILINE)
        if not match:
            raise TransferError(f"Could not parse gfal-stat size for {pfn}")
        return int(match.group(1))

    def checksum(self, pfn: str) -> str:
        result = self._run(("gfal-sum", pfn, "ADLER32"))
        fields = result.stdout.strip().split()
        if not fields:
            raise TransferError(f"Could not parse gfal-sum output for {pfn}")
        return _normalise_adler32(fields[-1])

    def validate(self, pfn: str, size: int, adler32: str) -> None:
        actual_size = self.stat_size(pfn)
        if actual_size is None:
            raise TransferError(f"File does not exist or is not readable: {pfn}")
        if actual_size != size:
            raise TransferError(
                f"Size mismatch for {pfn}: expected {size}, found {actual_size}"
            )
        actual_checksum = self.checksum(pfn)
        if actual_checksum != _normalise_adler32(adler32):
            raise TransferError(
                f"Checksum mismatch for {pfn}: expected {adler32}, found {actual_checksum}"
            )

    def dry_run_copy(self, item: ManifestFile) -> None:
        existing_size = self.stat_size(item.temp_pfn)
        if existing_size is not None:
            self.validate(item.temp_pfn, item.size, item.adler32)
            return
        self.validate(item.source_pfn, item.size, item.adler32)
        self._run(
            (
                "gfal-copy",
                "--dry-run",
                "--parent",
                "--copy-mode",
                "pull",
                "--checksum",
                f"ADLER32:{item.adler32}",
                "--checksum-mode",
                "both",
                item.source_pfn,
                item.temp_pfn,
            )
        )

    def ensure_temp_copy(self, item: ManifestFile) -> bool:
        existing_size = self.stat_size(item.temp_pfn)
        if existing_size is not None:
            self.validate(item.temp_pfn, item.size, item.adler32)
            return False
        self.validate(item.source_pfn, item.size, item.adler32)
        self._run(
            (
                "gfal-copy",
                "--parent",
                "--abort-on-failure",
                "--copy-mode",
                "pull",
                "--checksum",
                f"ADLER32:{item.adler32}",
                "--checksum-mode",
                "both",
                item.source_pfn,
                item.temp_pfn,
            )
        )
        self.validate(item.temp_pfn, item.size, item.adler32)
        return True


class DBSDatasetImporter:
    """Create and execute a resumable import manifest."""

    def __init__(
        self,
        rucio_client: Any,
        transfer: GfalTransfer | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        self.client = rucio_client
        self.transfer = transfer
        self.progress = progress

    def _report(self, message: str) -> None:
        LOGGER.info(message)
        if self.progress:
            self.progress(message)

    def preflight_transfers(self, manifest: ImportManifest, count: int) -> None:
        if count <= 0:
            return
        transfer = self.transfer or GfalTransfer()
        for item in manifest.files[:count]:
            self._report(f"Validating source and dry-run path: {item.source_lfn}")
            transfer.dry_run_copy(item)

    def execute(self, manifest: ImportManifest) -> str:
        existing_rule_id = self._matching_rule_id(manifest)
        quota = check_quota(self.client, manifest)
        if not quota.available and existing_rule_id is None:
            raise ImportPreflightError(quota.reason)
        transfer = self.transfer or GfalTransfer()

        self._report("Preparing Rucio container and block datasets")
        self._ensure_did(manifest.scope, manifest.container, "CONTAINER")
        for block in manifest.blocks:
            self._ensure_did(manifest.scope, block.name, "DATASET")
        self._attach_missing(
            manifest.scope,
            manifest.container,
            [
                {"scope": manifest.scope, "name": block.name}
                for block in manifest.blocks
            ],
        )
        rule_id = existing_rule_id or self._ensure_rule(manifest)
        self._report(f"Using replication rule {rule_id}")

        files_by_block: dict[str, list[ManifestFile]] = {}
        for item in manifest.files:
            files_by_block.setdefault(item.block, []).append(item)
        for block_number, block in enumerate(manifest.blocks, start=1):
            block_files = files_by_block[block.name]
            self._report(
                f"Importing block {block_number}/{len(manifest.blocks)} "
                f"({len(block_files)} files): {block.name}"
            )
            for batch in _chunks(block_files, 100):
                self._ensure_file_batch(manifest, block.name, list(batch), transfer)
            self.client.set_status(manifest.scope, block.name, open=False)
            self._report(
                f"Completed block {block_number}/{len(manifest.blocks)}: {block.name}"
            )
        self.client.set_status(manifest.scope, manifest.container, open=False)
        return rule_id

    def _ensure_did(self, scope: str, name: str, did_type: str) -> None:
        try:
            info = self.client.get_did(scope, name)
        except Exception as error:
            if _exception_name(error) not in {
                "DataIdentifierNotFound",
                "DataIdentifierNotFoundRSENotFound",
            }:
                raise
            self.client.add_did(scope=scope, name=name, did_type=did_type)
            return
        actual_type = _did_type_name(info.get("type") or info.get("did_type"))
        if actual_type != did_type:
            raise MetadataConflictError(
                f"{scope}:{name} exists as {actual_type}, expected {did_type}"
            )

    def _attach_missing(
        self,
        scope: str,
        parent: str,
        children: Sequence[Mapping[str, str]],
    ) -> None:
        if not children:
            return
        existing = {
            (str(item["scope"]), str(item["name"]))
            for item in self.client.list_content(scope, parent)
        }
        missing = [
            dict(item)
            for item in children
            if (str(item["scope"]), str(item["name"])) not in existing
        ]
        for batch in _chunks(missing, 1000):
            self.client.attach_dids(scope=scope, name=parent, dids=list(batch))

    def _ensure_rule(self, manifest: ImportManifest) -> str:
        existing = self._matching_rule_id(manifest)
        if existing is not None:
            return existing
        result = self.client.add_replication_rule(
            [{"scope": manifest.scope, "name": manifest.container}],
            manifest.copies,
            manifest.target_rse,
            lifetime=manifest.lifetime,
            grouping="DATASET",
        )
        return str(result[0])

    def _matching_rule_id(self, manifest: ImportManifest) -> str | None:
        try:
            rules = self.client.list_did_rules(manifest.scope, manifest.container)
        except Exception as error:
            if _exception_name(error) == "DataIdentifierNotFound":
                return None
            raise
        for rule in rules:
            if (
                str(rule.get("account")) == manifest.account
                and str(rule.get("rse_expression")) == manifest.target_rse
                and int(rule.get("copies", 0)) == manifest.copies
            ):
                return str(rule["id"])
        return None

    def _ensure_file_batch(
        self,
        manifest: ImportManifest,
        block_name: str,
        files: Sequence[ManifestFile],
        transfer: GfalTransfer,
    ) -> None:
        dids = [{"scope": manifest.scope, "name": item.target_lfn} for item in files]
        replica_records = list(self.client.list_replicas(dids, all_states=True))
        replica_by_name = {str(item["name"]): item for item in replica_records}
        replicas_to_add = []
        for item in files:
            existing = replica_by_name.get(item.target_lfn)
            existing_rses = set(existing.get("rses", {})) if existing else set()
            if existing:
                existing_size = int(existing.get("bytes") or 0)
                existing_adler32 = _normalise_adler32(existing.get("adler32"))
                if existing_size != item.size or existing_adler32 != item.adler32:
                    raise MetadataConflictError(
                        f"{manifest.scope}:{item.target_lfn} exists with different size/checksum"
                    )
            if (
                manifest.temp_rse not in existing_rses
                and manifest.target_rse not in existing_rses
            ):
                transfer.ensure_temp_copy(item)
                replicas_to_add.append(
                    {
                        "scope": manifest.scope,
                        "name": item.target_lfn,
                        "bytes": item.size,
                        "adler32": item.adler32,
                        "meta": {},
                        "state": "A",
                        "pfn": item.temp_pfn,
                    }
                )
        if replicas_to_add:
            self.client.add_replicas(
                rse=manifest.temp_rse,
                files=replicas_to_add,
                ignore_availability=False,
            )
        self._attach_missing(manifest.scope, block_name, dids)
