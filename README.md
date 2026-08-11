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
`cmsrucio_import/templates/dbs-dataset-import.yml`. Only `dataset` is required.
The safe minimal form is:

```yaml
kind: DBSDatasetImport
specs:
  dataset: /Primary/Processed/USER
  options:
    dryRun: true
    manifestPath: dbs-dataset-import-manifest.json
```

The importer derives the normal same-site values as follows:

- `dataset`: exact DBS `/primary/processed/tier` name;
- `dbsInstance` defaults to `phys03` for CRAB-published user datasets;
- `includeInvalidFiles` defaults to `false`; set it deliberately when the
  physical files still exist but DBS records were invalidated;
- `rucioScope` defaults to the account's sole owned scope; set it explicitly
  when the account owns more than one;
- the source RSE is the one common `origin_site_name` of the imported DBS
  blocks;
- the temporary RSE is `<source RSE>_Temp`, and the final RSE defaults to the
  source RSE;
- the source LFN prefix is the one common `/store/user/<owner>/` or
  `/store/group/<group>/` root;
- the target LFN prefix follows the scope type:
  `/store/{user,group}/rucio/$RUCIO_ACCOUNT/`;
- user-scope imports default to temporary prefix
  `/store/temp/user/rucio/$RUCIO_ACCOUNT/`; group imports require an explicit
  `destination.tempLFNPrefix` below the submitting user's `/store/temp/user/`
  area;
- `collection.containerName`: optional; defaults to the DBS dataset name;
- `rule.copies` defaults to one; an omitted `rule.lifetime` creates a permanent
  rule;
- `options.dryRun`: defaults to `true`;
- `options.manifestPath`: optional JSON manifest destination;
- `options.preflightFiles`: number of source files for which to verify size,
  checksum, and a `gfal-copy --dry-run` path.

`source`, `destination`, `lfnRewrite`, `collection`, and `rule` remain optional
overrides. An explicit `lfnRewrite.from` is needed only if the files do not use
one standard user/group root. `lfnRewrite.to`, when supplied, must still equal
the authenticated account's managed namespace.

Source and target ownership are intentionally separate. With personal account
`clange`, for example:

```text
/store/group/cmst3/group/hplushf/NanoTuples/...
    -> /store/user/rucio/clange/group/hplushf/NanoTuples/...
```

Creating a group-owned target namespace is a different authorization model and
is not inferred merely because the legacy files live below `/store/group`. It
requires authentication as the group account and a group scope owned by that
account. For example, account `t2_ch_cscs_local_users` with scope
`group.t2_ch_cscs` maps to:

```text
/store/group/rucio/t2_ch_cscs_local_users/...
```

The corresponding temporary PFN must not use `/store/temp/group`. CMS group
output is staged through a personal CRAB-style prefix, independently of its
final group DID name:

```yaml
destination:
  tempLFNPrefix: /store/temp/user/<username>.<DN-hash>/dataset-import/
```

`gfal-copy --dry-run` validates the resolved URLs but does not test WebDAV
write authorization. A real copy can still fail if the configured personal
temporary prefix is not writable by the proxy identity.

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
