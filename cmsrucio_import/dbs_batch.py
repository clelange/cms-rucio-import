"""Resumable, sequential orchestration for DBS dataset imports."""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .dbs_import import (
    DBSDatasetImporter,
    DBSReader,
    ImportConfig,
    ImportManifest,
    build_manifest,
    check_quota,
    find_matching_rule,
)

BATCH_VERSION = 1
_DATASET_RE = re.compile(r"^/[^/]+/[^/]+/[^/]+$")
_TEMP_OWNER_RE = re.compile(r"^(/store/temp/user/[^/]+/)")


class BatchImportError(RuntimeError):
    """A batch could not be planned, resumed, or completed safely."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_dataset_list(source: str | Path) -> list[str]:
    datasets = []
    for line_number, raw_line in enumerate(Path(source).read_text().splitlines(), 1):
        dataset = raw_line.strip()
        if not dataset or dataset.startswith("#"):
            continue
        if not _DATASET_RE.fullmatch(dataset):
            raise BatchImportError(
                f"Invalid DBS dataset on line {line_number}: {dataset}"
            )
        datasets.append(dataset)
    if not datasets:
        raise BatchImportError("The dataset list is empty")
    duplicates = sorted(
        dataset for dataset in set(datasets) if datasets.count(dataset) > 1
    )
    if duplicates:
        raise BatchImportError(f"Duplicate dataset in input: {duplicates[0]}")
    return datasets


def infer_temp_lfn_root(
    base_specs: Mapping[str, Any], configured_root: str | None = None
) -> str:
    base_prefix = str(
        base_specs.get("destination", {}).get("tempLFNPrefix", "")
    )
    base_owner = _TEMP_OWNER_RE.match(base_prefix)
    if configured_root:
        root = configured_root
    else:
        if not base_owner:
            raise BatchImportError(
                "Cannot infer the batch temporary LFN root; pass --temp-lfn-root "
                "below the submitting user's /store/temp/user area"
            )
        root = f"{base_owner.group(1)}cmsrucio-import/"
    if not root.endswith("/"):
        root += "/"
    root_owner = _TEMP_OWNER_RE.match(root)
    if not root_owner or any(part in {".", ".."} for part in Path(root).parts):
        raise BatchImportError(
            "The batch temporary LFN root must be below /store/temp/user/<owner>/"
        )
    if base_owner and root_owner.group(1) != base_owner.group(1):
        raise BatchImportError(
            "The batch temporary LFN root must use the same submitting user "
            "area as the base configuration"
        )
    return root


def dataset_slug(dataset: str, index: int) -> str:
    primary = dataset.split("/", 2)[1].lower()
    readable = re.sub(r"[^a-z0-9]+", "-", primary).strip("-")[:48]
    digest = hashlib.sha256(dataset.encode()).hexdigest()[:10]
    return f"{index:03d}-{readable or 'dataset'}-{digest}"


def _yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if value is None:
        return "null"
    return json.dumps(str(value))


def render_yaml(payload: Mapping[str, Any], indent: int = 0) -> str:
    lines = []
    prefix = " " * indent
    for key, value in payload.items():
        if isinstance(value, Mapping):
            lines.append(f"{prefix}{key}:")
            lines.append(render_yaml(value, indent + 2).rstrip())
        else:
            lines.append(f"{prefix}{key}: {_yaml_scalar(value)}")
    return "\n".join(lines) + "\n"


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _batch_lock(state_path: Path):
    lock_path = state_path.parent / "batch.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BatchImportError(
                f"Batch {state_path} is already being run or refreshed"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def load_batch_state(source: str | Path) -> dict[str, Any]:
    path = Path(source)
    try:
        state = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise BatchImportError(f"Could not read batch state {path}: {error}") from error
    if state.get("version") != BATCH_VERSION:
        raise BatchImportError(
            f"Unsupported batch state version {state.get('version')!r}"
        )
    return state


def _write_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = _now()
    _atomic_write_json(path, state)
    _write_rules_tsv(path.parent / "rules.tsv", state["entries"])


def _write_rules_tsv(path: Path, entries: list[dict[str, Any]]) -> None:
    columns = (
        "index",
        "dataset",
        "status",
        "rule_id",
        "expected_locks",
        "rule_state",
        "locks_ok",
        "locks_replicating",
        "locks_stuck",
        "files",
        "bytes",
    )
    rows = ["\t".join(columns)]
    for entry in entries:
        rule = entry.get("rule", {})
        values = {
            **entry,
            "rule_state": rule.get("state", ""),
            "locks_ok": rule.get("locks_ok", ""),
            "locks_replicating": rule.get("locks_replicating", ""),
            "locks_stuck": rule.get("locks_stuck", ""),
        }
        rows.append("\t".join(str(values.get(column, "")) for column in columns))
    path.write_text("\n".join(rows) + "\n")


def _rule_state_name(value: Any) -> str:
    name = getattr(value, "name", None)
    if name:
        return str(name).upper()
    value = str(getattr(value, "value", value) or "UNKNOWN").upper()
    return {
        "O": "OK",
        "R": "REPLICATING",
        "S": "STUCK",
        "U": "SUSPENDED",
        "W": "WAITING_APPROVAL",
        "I": "INJECT",
    }.get(value, value.split(".")[-1])


def get_rule_progress(
    rucio_client: Any, rule_id: str, expected_locks: int
) -> dict[str, Any]:
    rule = rucio_client.get_replication_rule(rule_id)
    progress = {
        "state": _rule_state_name(rule.get("state")),
        "locks_ok": int(rule.get("locks_ok_cnt") or 0),
        "locks_replicating": int(rule.get("locks_replicating_cnt") or 0),
        "locks_stuck": int(rule.get("locks_stuck_cnt") or 0),
        "checked_at": _now(),
    }
    progress["complete"] = (
        progress["state"] == "OK"
        and progress["locks_ok"] == expected_locks
        and progress["locks_replicating"] == 0
        and progress["locks_stuck"] == 0
    )
    return progress


def _prepare_specs(
    base_specs: Mapping[str, Any],
    dataset: str,
    temp_lfn_prefix: str,
    manifest_path: Path,
) -> dict[str, Any]:
    specs = copy.deepcopy(dict(base_specs))
    configured_container = specs.get("collection", {}).get("containerName")
    base_dataset = specs.get("dataset")
    if configured_container and configured_container != base_dataset:
        raise BatchImportError(
            "A batch base config cannot use a custom collection.containerName"
        )
    specs.pop("collection", None)
    specs["dataset"] = dataset
    specs.setdefault("destination", {})["tempLFNPrefix"] = temp_lfn_prefix
    options = specs.setdefault("options", {})
    options["dryRun"] = False
    options["manifestPath"] = str(manifest_path.resolve())
    return specs


def _manifest_path(state_path: Path, entry: Mapping[str, Any]) -> Path:
    return state_path.parent / str(entry["manifest_path"])


def _load_entry_manifest(
    state_path: Path, entry: Mapping[str, Any]
) -> ImportManifest:
    path = _manifest_path(state_path, entry)
    expected = entry.get("manifest_sha256")
    if not expected:
        raise BatchImportError(f"No manifest checksum recorded for {entry['dataset']}")
    try:
        actual = _sha256(path)
    except OSError as error:
        raise BatchImportError(f"Could not read manifest {path}: {error}") from error
    if actual != expected:
        raise BatchImportError(
            f"Manifest changed after planning for {entry['dataset']}: {path}"
        )
    try:
        return ImportManifest.read(path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise BatchImportError(f"Invalid manifest {path}: {error}") from error


def _refresh_entry(
    rucio_client: Any, state_path: Path, entry: dict[str, Any]
) -> dict[str, Any]:
    manifest = _load_entry_manifest(state_path, entry)
    rule_id = entry.get("rule_id")
    if not rule_id:
        matching = find_matching_rule(rucio_client, manifest)
        if matching:
            rule_id = str(matching["id"])
            entry["rule_id"] = rule_id
            entry.setdefault("origin", "existing")
    if not rule_id:
        entry.pop("rule", None)
        return entry
    progress = get_rule_progress(
        rucio_client, str(rule_id), int(entry["expected_locks"])
    )
    entry["rule"] = progress
    if progress["complete"]:
        if entry.get("origin") == "existing" and not entry.get("attempts"):
            entry["status"] = "complete-existing"
        else:
            entry["status"] = "complete"
        entry["registration_complete"] = True
        entry.pop("error", None)
        entry.pop("failure_phase", None)
    elif progress["locks_stuck"] or progress["state"] == "STUCK":
        entry["status"] = "rule-stuck"
    elif entry.get("registration_complete"):
        entry["status"] = "rule-wait"
    return entry


def _quota_groups(
    rucio_client: Any,
    state_path: Path,
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[ImportManifest]] = {}
    for entry in entries:
        if entry.get("status") in {"complete", "complete-existing"}:
            continue
        manifest = _load_entry_manifest(state_path, entry)
        grouped.setdefault((manifest.account, manifest.target_rse), []).append(
            manifest
        )
    quota_records = []
    for (account, target_rse), manifests in sorted(grouped.items()):
        required = sum(manifest.total_bytes for manifest in manifests)
        status = check_quota(
            rucio_client, manifests[0], required_bytes=required
        )
        record = asdict(status)
        record.update({"account": account, "target_rse": target_rse})
        quota_records.append(record)
    return quota_records


def plan_batch(
    datasets_file: str | Path,
    base_specs: Mapping[str, Any],
    workdir: str | Path,
    rucio_client: Any,
    *,
    temp_lfn_root: str | None = None,
    reader_factory: Callable[[str], Any] = DBSReader,
    transfer_factory: Callable[[], Any] | None = None,
    progress: Callable[[str], None] | None = None,
) -> Path:
    """Resolve all datasets, preflight them, and create an immutable batch plan."""

    report = progress or (lambda _message: None)
    datasets = read_dataset_list(datasets_file)
    root = Path(workdir)
    state_path = root / "batch-state.json"
    if state_path.exists():
        raise BatchImportError(
            f"Batch state already exists at {state_path}; resume it or use a new workdir"
        )
    for directory in ("configs", "manifests", "logs"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    normalised_list = "\n".join(datasets) + "\n"
    (root / "datasets.txt").write_text(normalised_list)
    (root / "base.yml").write_text(
        render_yaml({"kind": "DBSDatasetImport", "specs": base_specs})
    )
    temp_root = infer_temp_lfn_root(base_specs, temp_lfn_root)
    state: dict[str, Any] = {
        "version": BATCH_VERSION,
        "created_at": _now(),
        "updated_at": _now(),
        "status": "planning",
        "source_dataset_file": str(Path(datasets_file).resolve()),
        "temp_lfn_root": temp_root,
        "entries": [],
        "quota": [],
    }
    _write_state(state_path, state)

    total = len(datasets)
    for index, dataset in enumerate(datasets, 1):
        slug = dataset_slug(dataset, index)
        config_relative = Path("configs") / f"{slug}.yml"
        manifest_relative = Path("manifests") / f"{slug}.json"
        log_relative = Path("logs") / f"{slug}.log"
        entry: dict[str, Any] = {
            "index": index,
            "dataset": dataset,
            "slug": slug,
            "status": "planning",
            "config_path": str(config_relative),
            "manifest_path": str(manifest_relative),
            "log_path": str(log_relative),
            "attempts": 0,
            "registration_complete": False,
        }
        state["entries"].append(entry)
        _write_state(state_path, state)
        report(f"Planning dataset {index}/{total}: {dataset}")
        try:
            specs = _prepare_specs(
                base_specs,
                dataset,
                f"{temp_root}{slug}/",
                root / manifest_relative,
            )
            (root / config_relative).write_text(
                render_yaml(
                    {
                        "kind": "DBSDatasetImport",
                        "metadata": {"name": f"import-{slug}"},
                        "specs": specs,
                    }
                )
            )
            config = ImportConfig.from_specs(specs)
            manifest = build_manifest(
                config, reader_factory(config.dbs_instance), rucio_client
            )
            manifest.write(root / manifest_relative)
            entry.update(
                {
                    "account": manifest.account,
                    "scope": manifest.scope,
                    "target_rse": manifest.target_rse,
                    "files": len(manifest.files),
                    "blocks": len(manifest.blocks),
                    "bytes": manifest.total_bytes,
                    "expected_locks": len(manifest.files) * manifest.copies,
                    "preflight_files": config.preflight_files,
                    "manifest_sha256": _sha256(root / manifest_relative),
                    "status": "planned",
                }
            )
            matching = find_matching_rule(rucio_client, manifest)
            if matching:
                entry["rule_id"] = str(matching["id"])
                entry["origin"] = "existing"
                _refresh_entry(rucio_client, state_path, entry)
            if entry["status"] != "complete-existing" and config.preflight_files:
                transfer = transfer_factory() if transfer_factory else None
                DBSDatasetImporter(
                    rucio_client, transfer=transfer, progress=report
                ).preflight_transfers(manifest, config.preflight_files)
            _write_state(state_path, state)
        except Exception as error:
            entry["status"] = "plan-failed"
            entry["error"] = str(error)
            state["status"] = "plan-failed"
            _write_state(state_path, state)
            raise BatchImportError(
                f"Planning failed for dataset {index}/{total}: {error}"
            ) from error

    state["quota"] = _quota_groups(rucio_client, state_path, state["entries"])
    state["status"] = (
        "planned"
        if all(record["available"] for record in state["quota"])
        else "quota-insufficient"
    )
    _write_state(state_path, state)
    return state_path


def refresh_batch_status(
    state_file: str | Path,
    rucio_client: Any,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    state_path = Path(state_file)
    with _batch_lock(state_path):
        try:
            return _refresh_batch_status(
                state_path, rucio_client, progress=progress
            )
        except BatchImportError:
            raise
        except Exception as error:
            raise BatchImportError(f"Status refresh failed: {error}") from error


def _refresh_batch_status(
    state_file: str | Path,
    rucio_client: Any,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    state_path = Path(state_file)
    state = load_batch_state(state_path)
    report = progress or (lambda _message: None)
    for entry in state["entries"]:
        _refresh_entry(rucio_client, state_path, entry)
        rule = entry.get("rule", {})
        report(
            f"{entry['index']:03d} {entry['status']}: {entry['dataset']} "
            f"rule={entry.get('rule_id', '-')} "
            f"locks={rule.get('locks_ok', 0)}/"
            f"{rule.get('locks_replicating', 0)}/"
            f"{rule.get('locks_stuck', 0)}"
        )
    completed = sum(
        entry["status"] in {"complete", "complete-existing"}
        for entry in state["entries"]
    )
    if completed == len(state["entries"]):
        state["status"] = "complete"
    _write_state(state_path, state)
    return state


def run_batch(
    state_file: str | Path,
    rucio_client: Any,
    *,
    poll_seconds: int = 60,
    rule_timeout: int = 0,
    transfer_factory: Callable[[], Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    state_path = Path(state_file)
    with _batch_lock(state_path):
        try:
            return _run_batch(
                state_path,
                rucio_client,
                poll_seconds=poll_seconds,
                rule_timeout=rule_timeout,
                transfer_factory=transfer_factory,
                sleep=sleep,
                progress=progress,
            )
        except BatchImportError:
            raise
        except Exception as error:
            raise BatchImportError(f"Batch execution failed: {error}") from error


def _run_batch(
    state_file: str | Path,
    rucio_client: Any,
    *,
    poll_seconds: int = 60,
    rule_timeout: int = 0,
    transfer_factory: Callable[[], Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Run pending imports sequentially and wait for each rule to complete."""

    if poll_seconds < 1:
        raise BatchImportError("poll_seconds must be at least one second")
    if rule_timeout < 0:
        raise BatchImportError("rule_timeout cannot be negative")
    state_path = Path(state_file)
    state = load_batch_state(state_path)
    report = progress or (lambda _message: None)
    if any(entry["status"] == "plan-failed" for entry in state["entries"]):
        raise BatchImportError("The batch plan is incomplete; fix or recreate it")

    for entry in state["entries"]:
        _refresh_entry(rucio_client, state_path, entry)
    state["quota"] = _quota_groups(rucio_client, state_path, state["entries"])
    if any(not record["available"] for record in state["quota"]):
        state["status"] = "quota-insufficient"
        _write_state(state_path, state)
        detail = next(
            record for record in state["quota"] if not record["available"]
        )
        raise BatchImportError(
            f"Aggregate quota is insufficient on {detail['target_rse']}: "
            f"required {detail['required']} bytes"
        )
    state["status"] = "running"
    _write_state(state_path, state)

    total = len(state["entries"])
    for entry in state["entries"]:
        _refresh_entry(rucio_client, state_path, entry)
        if entry["status"] in {"complete", "complete-existing"}:
            report(
                f"Skipping completed dataset {entry['index']}/{total}: "
                f"{entry['dataset']}"
            )
            _write_state(state_path, state)
            continue
        manifest = _load_entry_manifest(state_path, entry)
        if str(rucio_client.account) != manifest.account:
            raise BatchImportError(
                f"Authenticated Rucio account {rucio_client.account} does not "
                f"match planned account {manifest.account}"
            )
        log_path = state_path.parent / entry["log_path"]
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as log:
            def entry_report(message: str) -> None:
                line = f"[{_now()}] {message}"
                log.write(line + "\n")
                log.flush()
                report(message)

            if not entry.get("registration_complete"):
                entry["status"] = "importing"
                entry["attempts"] = int(entry.get("attempts", 0)) + 1
                entry["started_at"] = _now()
                entry.pop("error", None)
                _write_state(state_path, state)
                entry_report(
                    f"Importing dataset {entry['index']}/{total}: {entry['dataset']}"
                )
                try:
                    transfer = transfer_factory() if transfer_factory else None
                    importer = DBSDatasetImporter(
                        rucio_client, transfer=transfer, progress=entry_report
                    )
                    preflight_files = int(entry.get("preflight_files", 0))
                    if preflight_files:
                        importer.preflight_transfers(manifest, preflight_files)
                    entry["rule_id"] = importer.execute(manifest)
                    entry["registration_complete"] = True
                    entry["status"] = "rule-wait"
                    entry["registered_at"] = _now()
                    entry.pop("failure_phase", None)
                    _write_state(state_path, state)
                except Exception as error:
                    entry["status"] = "failed"
                    entry["failure_phase"] = "import"
                    entry["error"] = str(error)
                    state["status"] = "failed"
                    _write_state(state_path, state)
                    entry_report(f"Import failed: {error}")
                    raise BatchImportError(
                        f"Import failed for {entry['dataset']}: {error}"
                    ) from error

            wait_started = time.monotonic()
            previous_counts = None
            while True:
                try:
                    progress_record = get_rule_progress(
                        rucio_client,
                        str(entry["rule_id"]),
                        int(entry["expected_locks"]),
                    )
                except Exception as error:
                    entry["status"] = "failed"
                    entry["failure_phase"] = "rule-status"
                    entry["error"] = str(error)
                    state["status"] = "failed"
                    _write_state(state_path, state)
                    raise BatchImportError(
                        f"Could not read rule {entry['rule_id']}: {error}"
                    ) from error
                entry["rule"] = progress_record
                entry["status"] = "rule-wait"
                counts = (
                    progress_record["state"],
                    progress_record["locks_ok"],
                    progress_record["locks_replicating"],
                    progress_record["locks_stuck"],
                )
                if counts != previous_counts:
                    entry_report(
                        f"Rule {entry['rule_id']} {counts[0]} locks "
                        f"OK/REPLICATING/STUCK={counts[1]}/{counts[2]}/{counts[3]} "
                        f"(expected {entry['expected_locks']})"
                    )
                    previous_counts = counts
                if progress_record["complete"]:
                    entry["status"] = "complete"
                    entry["completed_at"] = _now()
                    entry.pop("error", None)
                    entry.pop("failure_phase", None)
                    _write_state(state_path, state)
                    break
                if progress_record["locks_stuck"] or counts[0] == "STUCK":
                    entry["status"] = "rule-stuck"
                    entry["failure_phase"] = "rule"
                    entry["error"] = "Replication rule has stuck locks"
                    state["status"] = "failed"
                    _write_state(state_path, state)
                    raise BatchImportError(
                        f"Rule {entry['rule_id']} has stuck locks; stopping batch"
                    )
                if rule_timeout and time.monotonic() - wait_started >= rule_timeout:
                    entry["status"] = "rule-timeout"
                    entry["failure_phase"] = "rule"
                    entry["error"] = "Replication rule wait timed out"
                    state["status"] = "failed"
                    _write_state(state_path, state)
                    raise BatchImportError(
                        f"Timed out waiting for rule {entry['rule_id']}"
                    )
                _write_state(state_path, state)
                sleep(poll_seconds)

    state["status"] = "complete"
    state["completed_at"] = _now()
    _write_state(state_path, state)
    return state
