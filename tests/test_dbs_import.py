import unittest
from unittest.mock import Mock

from cmsrucio_import.dbs_import import (
    DBSBlock,
    DBSDatasetImporter,
    DBSFile,
    DBSReader,
    GfalTransfer,
    ImportConfig,
    ImportConfigurationError,
    build_manifest,
    check_quota,
)

DATASET = "/Primary/clange-Processed/USER"
BLOCK_A = f"{DATASET}#aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
BLOCK_B = f"{DATASET}#bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def make_config(**overrides):
    specs = {
        "dataset": DATASET,
        "dbsInstance": "phys03",
        "includeInvalidFiles": False,
        "source": {},
        "destination": {"tempRSE": "T3_CH_PSI_Temp", "rse": "T3_CH_PSI"},
        "lfnRewrite": {
            "from": "/store/user/clange/",
            "to": "/store/user/rucio/clange/",
        },
        "rule": {"copies": 1, "lifetime": 1200000},
        "options": {"dryRun": True, "preflightFiles": 1},
    }
    specs.update(overrides)
    return ImportConfig.from_specs(specs)


class FakeDBSReader:
    def list_blocks(self, dataset):
        self.requested_dataset = dataset
        return [
            DBSBlock(BLOCK_A, "T3_CH_PSI", 2, 30),
            DBSBlock(BLOCK_B, "T3_CH_PSI", 1, 30),
        ]

    def list_files(self, dataset, include_invalid=False):
        self.include_invalid = include_invalid
        return [
            DBSFile(
                "/store/user/clange/task/0000/tree_1.root",
                BLOCK_A,
                10,
                "00000001",
                True,
                100,
            ),
            DBSFile(
                "/store/user/clange/task/0001/tree_1.root",
                BLOCK_A,
                20,
                "00000002",
                True,
                200,
            ),
            DBSFile(
                "/store/user/clange/task/0001/tree_2.root",
                BLOCK_B,
                30,
                "00000003",
                True,
                300,
            ),
        ]


class FakeGroupDBSReader(FakeDBSReader):
    def list_blocks(self, dataset):
        self.requested_dataset = dataset
        return [
            DBSBlock(BLOCK_A, "T2_CH_CSCS", 2, 30),
            DBSBlock(BLOCK_B, "T2_CH_CSCS", 1, 30),
        ]

    def list_files(self, dataset, include_invalid=False):
        files = super().list_files(dataset, include_invalid)
        return [
            DBSFile(
                item.source_lfn.replace(
                    "/store/user/clange/", "/store/group/cmst3/group/hplushf/"
                ),
                item.block,
                item.size,
                item.adler32,
                item.valid,
                item.event_count,
            )
            for item in files
        ]


class DataIdentifierNotFound(Exception):
    pass


class FakeRucioClient:
    account = "clange"

    def __init__(self, quota=1000, account="clange"):
        self.account = account
        self.quota = quota
        self.dids = {}
        self.contents = {}
        self.rules = []
        self.replicas = {}
        self.replica_submissions = []
        self.statuses = {}
        self.status_calls = []
        self.open_dids = {}

    def list_scopes_for_account(self, account):
        if account == "t2_ch_cscs_local_users":
            return ["group.t2_ch_cscs"]
        return [f"user.{account}"]

    def get_rse(self, rse):
        if rse not in {
            "T2_CH_CSCS",
            "T2_CH_CSCS_Temp",
            "T3_CH_PSI",
            "T3_CH_PSI_Temp",
        }:
            raise RuntimeError("unknown RSE")
        return {
            "rse": rse,
            "deterministic": not rse.endswith("_Temp"),
            "availability_read": True,
            "availability_write": True,
        }

    def lfns2pfns(self, rse, dids, operation):
        return {did: "davs://t3se01.psi.ch:2880" + did.split(":", 1)[1] for did in dids}

    def get_local_account_limits(self, account):
        return {} if self.quota is None else {"T3_CH_PSI": self.quota}

    def get_local_account_usage(self, account, rse=None):
        return iter([{"bytes": 100}]) if self.quota is not None else iter([])

    def get_global_account_limits(self, account):
        return {}

    def get_global_account_usage(self, account, rse_expression=None):
        return iter([])

    def list_rses(self, expression):
        return iter([])

    def get_did(self, scope, name):
        if (scope, name) not in self.dids:
            raise DataIdentifierNotFound(name)
        return {
            "type": self.dids[(scope, name)],
            "open": self.open_dids[(scope, name)],
        }

    def add_did(self, scope, name, did_type):
        self.dids[(scope, name)] = did_type
        self.contents[(scope, name)] = []
        self.open_dids[(scope, name)] = True
        return True

    def list_content(self, scope, name):
        return iter(self.contents[(scope, name)])

    def attach_dids(self, scope, name, dids):
        self.contents[(scope, name)].extend(dict(item) for item in dids)
        return True

    def list_did_rules(self, scope, name):
        return iter(self.rules)

    def add_replication_rule(self, dids, copies, rse, **kwargs):
        self.rules.append(
            {
                "id": "rule-1",
                "account": self.account,
                "rse_expression": rse,
                "copies": copies,
            }
        )
        return ["rule-1"]

    def list_replicas(self, dids, all_states=False):
        records = []
        for did in dids:
            item = self.replicas.get((did["scope"], did["name"]))
            if item:
                records.append(item)
        return iter(records)

    def add_replicas(self, rse, files, ignore_availability=False):
        for item in files:
            self.replica_submissions.append(dict(item))
            self.replicas[(item["scope"], item["name"])] = {
                "scope": item["scope"],
                "name": item["name"],
                "bytes": item["bytes"],
                "adler32": item["adler32"],
                "rses": {rse: [item["pfn"]]},
            }
        return True

    def set_status(self, scope, name, **kwargs):
        key = (scope, name)
        if "open" in kwargs:
            if self.open_dids[key] == kwargs["open"]:
                raise RuntimeError(f"DID {scope}:{name} already has status {kwargs}")
            self.open_dids[key] = kwargs["open"]
        self.statuses[(scope, name)] = kwargs
        self.status_calls.append((scope, name, kwargs))
        return True


class FakeTransfer:
    def __init__(self):
        self.copied = []
        self.dry_runs = []

    def ensure_temp_copy(self, item):
        self.copied.append(item.target_lfn)
        return True

    def dry_run_copy(self, item):
        self.dry_runs.append(item.target_lfn)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


class ManifestTests(unittest.TestCase):
    def test_dbs_reader_requests_validity_and_normalises_checksum(self):
        session = FakeSession(
            [
                {
                    "logical_file_name": "/store/user/clange/task/file.root",
                    "block_name": BLOCK_A,
                    "file_size": 42,
                    "adler32": "abc",
                    "is_file_valid": 0,
                    "event_count": 7,
                }
            ]
        )
        reader = DBSReader(
            "phys03", session=session, proxy_path="/tmp/proxy", verify=False
        )

        files = reader.list_files(DATASET, include_invalid=True)

        self.assertEqual(files[0].adler32, "00000abc")
        self.assertFalse(files[0].valid)
        _, options = session.calls[0]
        self.assertEqual(options["params"]["validFileOnly"], 0)
        self.assertEqual(options["cert"], ("/tmp/proxy", "/tmp/proxy"))

    def test_manifest_preserves_blocks_paths_and_metadata(self):
        client = FakeRucioClient()
        reader = FakeDBSReader()
        manifest = build_manifest(make_config(), reader, client)

        self.assertEqual(manifest.source_rse, "T3_CH_PSI")
        self.assertEqual(manifest.scope, "user.clange")
        self.assertEqual(len(manifest.blocks), 2)
        self.assertEqual(len(manifest.files), 3)
        self.assertEqual(manifest.total_bytes, 60)
        first = next(
            item
            for item in manifest.files
            if item.source_lfn.endswith("0000/tree_1.root")
        )
        self.assertEqual(
            first.target_lfn,
            "/store/user/rucio/clange/task/0000/tree_1.root",
        )
        self.assertEqual(
            first.temp_pfn,
            "davs://t3se01.psi.ch:2880/store/temp/user/rucio/clange/"
            "task/0000/tree_1.root",
        )
        self.assertEqual(first.event_count, 100)

    def test_group_config_separates_personal_temp_and_group_target(self):
        temp_prefix = (
            "/store/temp/user/clange.examplehash/cmsrucio-dataset-import/"
        )
        config = ImportConfig.from_specs(
            {
                "dataset": DATASET,
                "destination": {"tempLFNPrefix": temp_prefix},
            }
        )
        client = FakeRucioClient(account="t2_ch_cscs_local_users")
        manifest = build_manifest(config, FakeGroupDBSReader(), client)

        self.assertTrue(config.dry_run)
        self.assertEqual(config.dbs_instance, "phys03")
        self.assertIsNone(config.lifetime)
        self.assertEqual(manifest.scope, "group.t2_ch_cscs")
        self.assertEqual(manifest.source_rse, "T2_CH_CSCS")
        self.assertEqual(manifest.temp_rse, "T2_CH_CSCS_Temp")
        self.assertEqual(manifest.target_rse, "T2_CH_CSCS")
        self.assertEqual(manifest.source_lfn_prefix, "/store/group/cmst3/")
        self.assertEqual(manifest.temp_lfn_prefix, temp_prefix)
        self.assertEqual(
            manifest.target_lfn_prefix,
            "/store/group/rucio/t2_ch_cscs_local_users/",
        )
        self.assertEqual(
            manifest.files[0].target_lfn,
            "/store/group/rucio/t2_ch_cscs_local_users/group/hplushf/"
            "task/0000/tree_1.root",
        )
        self.assertEqual(
            manifest.files[0].temp_pfn,
            "davs://t3se01.psi.ch:2880/store/temp/user/clange.examplehash/"
            "cmsrucio-dataset-import/group/hplushf/task/0000/tree_1.root",
        )

    def test_group_import_requires_personal_temp_prefix(self):
        config = ImportConfig.from_specs({"dataset": DATASET})
        client = FakeRucioClient(account="t2_ch_cscs_local_users")
        with self.assertRaises(ImportConfigurationError):
            build_manifest(config, FakeGroupDBSReader(), client)

    def test_group_temp_namespace_is_rejected(self):
        with self.assertRaises(ImportConfigurationError):
            ImportConfig.from_specs(
                {
                    "dataset": DATASET,
                    "destination": {
                        "tempLFNPrefix": "/store/temp/group/rucio/example/"
                    },
                }
            )

    def test_configured_scope_must_belong_to_account(self):
        config = make_config(rucioScope="group.t2_ch_cscs")
        with self.assertRaises(ImportConfigurationError):
            build_manifest(config, FakeDBSReader(), FakeRucioClient())

    def test_wrong_temp_rse_is_rejected(self):
        config = make_config(
            source={"rse": "T3_CH_PSI"},
            destination={"tempRSE": "T2_CH_CERN_Temp", "rse": "T3_CH_PSI"},
        )
        with self.assertRaises(ImportConfigurationError):
            build_manifest(config, FakeDBSReader(), FakeRucioClient())

    def test_quota_check_reports_capacity_and_absence(self):
        manifest = build_manifest(make_config(), FakeDBSReader(), FakeRucioClient())
        status = check_quota(FakeRucioClient(quota=1000), manifest)
        self.assertTrue(status.available)
        self.assertEqual(status.required, 60)

        missing = check_quota(FakeRucioClient(quota=None), manifest)
        self.assertFalse(missing.available)
        self.assertIn("no quota", missing.reason)

    def test_copy_preflight_validates_an_existing_temporary_file(self):
        item = build_manifest(
            make_config(), FakeDBSReader(), FakeRucioClient()
        ).files[0]
        transfer = object.__new__(GfalTransfer)
        transfer.stat_size = Mock(return_value=item.size)
        transfer.validate = Mock()
        transfer._run = Mock()

        transfer.dry_run_copy(item)

        transfer.stat_size.assert_called_once_with(item.temp_pfn)
        transfer.validate.assert_called_once_with(
            item.temp_pfn, item.size, item.adler32
        )
        transfer._run.assert_not_called()

    def test_copy_preflight_dry_runs_when_temporary_file_is_absent(self):
        item = build_manifest(
            make_config(), FakeDBSReader(), FakeRucioClient()
        ).files[0]
        transfer = object.__new__(GfalTransfer)
        transfer.stat_size = Mock(return_value=None)
        transfer.validate = Mock()
        transfer._run = Mock()

        transfer.dry_run_copy(item)

        transfer.validate.assert_called_once_with(
            item.source_pfn, item.size, item.adler32
        )
        command = transfer._run.call_args.args[0]
        self.assertIn("--dry-run", command)
        self.assertEqual(command[-2:], (item.source_pfn, item.temp_pfn))

    def test_execute_creates_hierarchy_rule_and_replicas(self):
        client = FakeRucioClient()
        manifest = build_manifest(make_config(), FakeDBSReader(), client)
        transfer = FakeTransfer()
        importer = DBSDatasetImporter(client, transfer)

        importer.preflight_transfers(manifest, 1)
        rule_id = importer.execute(manifest)

        self.assertEqual(rule_id, "rule-1")
        self.assertEqual(len(transfer.dry_runs), 1)
        self.assertEqual(len(transfer.copied), 3)
        self.assertEqual(client.dids[(manifest.scope, manifest.container)], "CONTAINER")
        self.assertEqual(client.dids[(manifest.scope, BLOCK_A)], "DATASET")
        self.assertEqual(len(client.contents[(manifest.scope, manifest.container)]), 2)
        self.assertEqual(len(client.contents[(manifest.scope, BLOCK_A)]), 2)
        self.assertEqual(len(client.replicas), 3)
        self.assertTrue(client.replica_submissions)
        self.assertTrue(
            all("md5" not in item for item in client.replica_submissions)
        )
        self.assertEqual(
            client.statuses[(manifest.scope, manifest.container)], {"open": False}
        )

    def test_resume_reuses_existing_rule_and_replicas_without_new_quota(self):
        client = FakeRucioClient()
        manifest = build_manifest(make_config(), FakeDBSReader(), client)
        first_transfer = FakeTransfer()
        DBSDatasetImporter(client, first_transfer).execute(manifest)
        client.quota = None
        resumed_transfer = FakeTransfer()
        status_calls = list(client.status_calls)

        rule_id = DBSDatasetImporter(client, resumed_transfer).execute(manifest)

        self.assertEqual(rule_id, "rule-1")
        self.assertEqual(resumed_transfer.copied, [])
        self.assertEqual(len(client.rules), 1)
        self.assertEqual(client.status_calls, status_calls)


if __name__ == "__main__":
    unittest.main()
