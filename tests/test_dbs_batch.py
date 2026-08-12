import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cmsrucio_import.dbs_batch import (
    BatchImportError,
    get_rule_progress,
    load_batch_state,
    plan_batch,
    read_dataset_list,
    run_batch,
)
from cmsrucio_import.dbs_import import ImportManifest, ManifestBlock, ManifestFile

DATASET_ONE = "/One/Processed/USER"
DATASET_TWO = "/Two/Processed/USER"
ACCOUNT = "t2_ch_cscs_local_users"
SCOPE = "group.t2_ch_cscs"
TARGET_RSE = "T2_CH_CSCS"


def make_manifest(config):
    block = f"{config.dataset}#aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    files = tuple(
        ManifestFile(
            source_lfn=f"/store/group/cmst3/source/file_{number}.root",
            target_lfn=(
                f"/store/group/rucio/{ACCOUNT}/source/"
                f"{config.dataset.split('/')[1]}/file_{number}.root"
            ),
            block=block,
            size=size,
            adler32=f"{number:08x}",
            valid=True,
            source_pfn=f"davs://storage.example/source/file_{number}.root",
            temp_pfn=f"davs://storage.example{config.temp_lfn_prefix}file_{number}.root",
            event_count=number,
        )
        for number, size in ((1, 10), (2, 20))
    )
    return ImportManifest(
        version=3,
        created_at="2026-08-12T00:00:00+00:00",
        account=ACCOUNT,
        scope=SCOPE,
        dataset=config.dataset,
        dbs_instance=config.dbs_instance,
        include_invalid_files=False,
        container=config.dataset,
        source_rse=TARGET_RSE,
        temp_rse=f"{TARGET_RSE}_Temp",
        target_rse=TARGET_RSE,
        source_lfn_prefix="/store/group/cmst3/",
        temp_lfn_prefix=config.temp_lfn_prefix,
        target_lfn_prefix=f"/store/group/rucio/{ACCOUNT}/",
        copies=1,
        lifetime=None,
        blocks=(ManifestBlock(block, TARGET_RSE, 2, 30),),
        files=files,
    )


def base_specs():
    return {
        "dataset": DATASET_ONE,
        "dbsInstance": "phys03",
        "rucioScope": SCOPE,
        "source": {"rse": TARGET_RSE},
        "destination": {
            "tempRSE": f"{TARGET_RSE}_Temp",
            "tempLFNPrefix": (
                "/store/temp/user/clange.hash/cmsrucio-import-one/"
            ),
            "rse": TARGET_RSE,
        },
        "rule": {"copies": 1},
        "options": {"dryRun": False, "preflightFiles": 1},
    }


class FakeTransfer:
    def __init__(self):
        self.preflighted = []
        self.copied = []

    def dry_run_copy(self, item):
        self.preflighted.append(item.target_lfn)

    def ensure_temp_copy(self, item):
        self.copied.append(item.target_lfn)
        return True


class DataIdentifierNotFound(Exception):
    pass


class FakeBatchRucioClient:
    account = ACCOUNT

    def __init__(self, existing_complete=True):
        self.dids = {}
        self.contents = {}
        self.replicas = {}
        self.rule_by_container = {}
        self.rules = {}
        self.new_rule_count = 0
        if existing_complete:
            self.rule_by_container[DATASET_ONE] = "existing-rule"
            self.rules["existing-rule"] = {
                "id": "existing-rule",
                "account": ACCOUNT,
                "scope": SCOPE,
                "name": DATASET_ONE,
                "rse_expression": TARGET_RSE,
                "copies": 1,
                "state": "OK",
                "locks_ok_cnt": 2,
                "locks_replicating_cnt": 0,
                "locks_stuck_cnt": 0,
            }

    def get_local_account_limits(self, account):
        return {TARGET_RSE: 10_000}

    def get_local_account_usage(self, account, rse=None):
        return iter([{"bytes": 100}])

    def get_global_account_limits(self, account):
        return {}

    def get_global_account_usage(self, account, rse_expression=None):
        return iter([])

    def list_rses(self, expression):
        return iter([])

    def list_did_rules(self, scope, name):
        rule_id = self.rule_by_container.get(name)
        return iter([self.rules[rule_id]] if rule_id else [])

    def get_replication_rule(self, rule_id):
        return dict(self.rules[rule_id])

    def get_did(self, scope, name):
        if (scope, name) not in self.dids:
            raise DataIdentifierNotFound(name)
        return {"type": self.dids[(scope, name)]}

    def add_did(self, scope, name, did_type):
        self.dids[(scope, name)] = did_type
        self.contents[(scope, name)] = []

    def list_content(self, scope, name):
        return iter(self.contents[(scope, name)])

    def attach_dids(self, scope, name, dids):
        self.contents[(scope, name)].extend(dict(item) for item in dids)

    def add_replication_rule(self, dids, copies, rse, **kwargs):
        self.new_rule_count += 1
        rule_id = f"new-rule-{self.new_rule_count}"
        container = dids[0]["name"]
        self.rule_by_container[container] = rule_id
        self.rules[rule_id] = {
            "id": rule_id,
            "account": ACCOUNT,
            "scope": SCOPE,
            "name": container,
            "rse_expression": rse,
            "copies": copies,
            "state": "OK",
            "locks_ok_cnt": 2,
            "locks_replicating_cnt": 0,
            "locks_stuck_cnt": 0,
        }
        return [rule_id]

    def list_replicas(self, dids, all_states=False):
        return iter(
            self.replicas[(did["scope"], did["name"])]
            for did in dids
            if (did["scope"], did["name"]) in self.replicas
        )

    def add_replicas(self, rse, files, ignore_availability=False):
        for item in files:
            self.replicas[(item["scope"], item["name"])] = {
                **item,
                "rses": {rse: [item["pfn"]]},
            }

    def set_status(self, scope, name, **kwargs):
        return None


class BatchTests(unittest.TestCase):
    def test_dataset_list_rejects_duplicates(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "datasets.txt"
            source.write_text(f"{DATASET_ONE}\n{DATASET_ONE}\n")

            with self.assertRaises(BatchImportError):
                read_dataset_list(source)

    def test_ok_rule_requires_all_expected_locks(self):
        client = FakeBatchRucioClient()
        client.rules["existing-rule"]["locks_ok_cnt"] = 0
        client.rules["existing-rule"]["state"] = "O"

        progress = get_rule_progress(client, "existing-rule", expected_locks=2)

        self.assertEqual(progress["state"], "OK")
        self.assertFalse(progress["complete"])

    def test_plan_skips_complete_existing_rule_and_checks_pending_quota(self):
        client = FakeBatchRucioClient()
        transfers = []

        def transfer_factory():
            transfer = FakeTransfer()
            transfers.append(transfer)
            return transfer

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.txt"
            source.write_text(f"{DATASET_ONE}\n{DATASET_TWO}\n")
            with patch(
                "cmsrucio_import.dbs_batch.build_manifest",
                side_effect=lambda config, _reader, _client: make_manifest(config),
            ):
                state_path = plan_batch(
                    source,
                    base_specs(),
                    root / "batch",
                    client,
                    reader_factory=lambda _instance: object(),
                    transfer_factory=transfer_factory,
                )

            state = load_batch_state(state_path)
            first, second = state["entries"]
            self.assertEqual(first["status"], "complete-existing")
            self.assertEqual(first["rule_id"], "existing-rule")
            self.assertEqual(second["status"], "planned")
            self.assertNotEqual(
                first["manifest_path"], second["manifest_path"]
            )
            self.assertEqual(state["quota"][0]["required"], 30)
            self.assertEqual(len(transfers), 1)
            self.assertEqual(len(transfers[0].preflighted), 1)
            self.assertIn("existing-rule", (root / "batch/rules.tsv").read_text())
            config_text = (
                root / "batch" / second["config_path"]
            ).read_text()
            self.assertIn("002-two-", config_text)

    def test_run_imports_pending_dataset_and_preserves_existing_rule(self):
        client = FakeBatchRucioClient()
        transfers = []

        def transfer_factory():
            transfer = FakeTransfer()
            transfers.append(transfer)
            return transfer

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.txt"
            source.write_text(f"{DATASET_ONE}\n{DATASET_TWO}\n")
            with patch(
                "cmsrucio_import.dbs_batch.build_manifest",
                side_effect=lambda config, _reader, _client: make_manifest(config),
            ):
                state_path = plan_batch(
                    source,
                    base_specs(),
                    root / "batch",
                    client,
                    reader_factory=lambda _instance: object(),
                    transfer_factory=transfer_factory,
                )

            state = run_batch(
                state_path,
                client,
                poll_seconds=1,
                transfer_factory=transfer_factory,
                sleep=lambda _seconds: None,
            )

            first, second = state["entries"]
            self.assertEqual(first["status"], "complete-existing")
            self.assertEqual(first["rule_id"], "existing-rule")
            self.assertEqual(second["status"], "complete")
            self.assertEqual(second["rule_id"], "new-rule-1")
            self.assertEqual(client.new_rule_count, 1)
            self.assertEqual(len(transfers[-1].copied), 2)
            on_disk = json.loads(Path(state_path).read_text())
            self.assertEqual(on_disk["status"], "complete")

    def test_run_resumes_at_rule_wait_without_reimporting(self):
        client = FakeBatchRucioClient()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.txt"
            source.write_text(f"{DATASET_TWO}\n")
            with patch(
                "cmsrucio_import.dbs_batch.build_manifest",
                side_effect=lambda config, _reader, _client: make_manifest(config),
            ):
                state_path = plan_batch(
                    source,
                    base_specs(),
                    root / "batch",
                    client,
                    reader_factory=lambda _instance: object(),
                    transfer_factory=FakeTransfer,
                )

            state = load_batch_state(state_path)
            entry = state["entries"][0]
            entry.update(
                {
                    "registration_complete": True,
                    "rule_id": "resumed-rule",
                    "status": "rule-wait",
                }
            )
            Path(state_path).write_text(json.dumps(state))
            client.rule_by_container[DATASET_TWO] = "resumed-rule"
            client.rules["resumed-rule"] = {
                "id": "resumed-rule",
                "account": ACCOUNT,
                "scope": SCOPE,
                "name": DATASET_TWO,
                "rse_expression": TARGET_RSE,
                "copies": 1,
                "state": "OK",
                "locks_ok_cnt": 2,
                "locks_replicating_cnt": 0,
                "locks_stuck_cnt": 0,
            }

            result = run_batch(
                state_path,
                client,
                poll_seconds=1,
                transfer_factory=lambda: self.fail(
                    "a completed registration must not run transfers again"
                ),
                sleep=lambda _seconds: None,
            )

            self.assertEqual(result["entries"][0]["status"], "complete")
            self.assertEqual(client.new_rule_count, 0)

    def test_run_rejects_a_manifest_changed_after_planning(self):
        client = FakeBatchRucioClient(existing_complete=False)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.txt"
            source.write_text(f"{DATASET_TWO}\n")
            with patch(
                "cmsrucio_import.dbs_batch.build_manifest",
                side_effect=lambda config, _reader, _client: make_manifest(config),
            ):
                state_path = plan_batch(
                    source,
                    base_specs(),
                    root / "batch",
                    client,
                    reader_factory=lambda _instance: object(),
                    transfer_factory=FakeTransfer,
                )
            state = load_batch_state(state_path)
            manifest_path = root / "batch" / state["entries"][0]["manifest_path"]
            manifest_path.write_text(manifest_path.read_text() + "\n")

            with self.assertRaisesRegex(BatchImportError, "Manifest changed"):
                run_batch(
                    state_path,
                    client,
                    poll_seconds=1,
                    transfer_factory=FakeTransfer,
                    sleep=lambda _seconds: None,
                )


if __name__ == "__main__":
    unittest.main()
