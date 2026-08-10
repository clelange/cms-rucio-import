# CMS Rucio import tools

This package has two upload paths:

- upload local files or a local directory through a CMS temporary RSE;
- import a legacy CRAB dataset registered in DBS by copying its existing files
  into the Rucio-managed user namespace.

## Requirements

Run data transfers on `lxplus` or another CMS UI host with `gfal-copy`,
`gfal-stat`, and `gfal-sum` available. Initialise Rucio and a CMS proxy, then
install the Python dependencies:

```shell
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh
voms-proxy-init -voms cms -rfc -valid 192:00
export RUCIO_ACCOUNT="<CMS Rucio account>"

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

From this directory, list commands with:

```shell
python -m cmsrucio_import --help
```

## Import a DBS dataset

Copy and edit
`cmsrucio_import/templates/dbs-dataset-import.yml`. The important fields are:

- `dataset`: exact DBS `/primary/processed/tier` name;
- `dbsInstance`: normally `phys03` for CRAB-published user datasets;
- `includeInvalidFiles`: defaults to `false`; set it deliberately when the
  physical files still exist but DBS records were invalidated;
- `source.rse`: the RSE holding the legacy `/store/user/<name>/...` files. It
  can be omitted only when every imported DBS block has one common RSE-like
  `origin_site_name`;
- `destination.tempRSE`: the source site's paired temporary RSE, for example
  `T3_CH_PSI_Temp` for `T3_CH_PSI`;
- `destination.rse`: the final RSE on which the replication rule is created;
- `lfnRewrite`: replaces the legacy user prefix while preserving the complete
  CRAB directory structure below it;
- `collection.containerName`: optional; defaults to the DBS dataset name;
- `rule`: one target copy and its lifetime in seconds;
- `options.dryRun`: defaults to `true`;
- `options.manifestPath`: optional JSON manifest destination;
- `options.preflightFiles`: number of source files for which to verify size,
  checksum, and a `gfal-copy --dry-run` path.

Run a dry import with:

```shell
python -m cmsrucio_import import-dbs-dataset-yaml my-import.yml
```

Inspect the manifest and quota result. To execute, change `dryRun` to `false`
and run the same command. A real import refuses to start without sufficient
Rucio quota on the target RSE.

The importer performs these operations:

1. Queries DBSReader for blocks, files, sizes, validity, and Adler-32 values.
2. Resolves the legacy source PFNs and `/store/temp/user/rucio/...` PFNs using
   the CMS Rucio TFC.
3. Creates a Rucio container named after the DBS dataset and one Rucio dataset
   for each imported DBS block.
4. Creates the target replication rule.
5. Copies each source file to the temporary RSE with size and checksum
   validation, registers the temporary replica in `user.$RUCIO_ACCOUNT`, and
   attaches it to its block dataset.
6. Closes the block datasets and container after all files are registered.

The operation is resumable. Existing collections, rules, and replicas are
reused only when their types, sizes, and checksums match. Metadata conflicts
stop the import instead of being ignored.

Do not copy files directly into `/store/user/rucio/...`; that namespace is
managed by Rucio. The importer writes to the source site's temporary RSE and
lets the target rule create the managed replica.

## Upload local files

The original local upload commands remain available:

```shell
python -m cmsrucio_import upload-file-yaml my-file.yml
python -m cmsrucio_import upload-dataset-yaml my-directory.yml
```

The local directory uploader creates one Rucio dataset from top-level files in
`datasetPath`; it does not query DBS or build a block/container hierarchy.
