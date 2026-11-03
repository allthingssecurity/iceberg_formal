# iceberg-verify

Formal verification of Apache Iceberg's commit protocol, laid out like `camel-lean/tla` so the same
runner and trace scripts work.

```
tla/run2.sh commit IcebergCommit <cfg>     # run one config, writes tla/commit/out/<cfg>.log
python3 tla/gencfg.py commit               # regenerate .cfg from tla/commit/configs.py
python3 tla/trace.py commit <cfg> [vars]   # print the counterexample, one line per state
```

Run everything:

```
cd tla && for c in $(cat commit/cfglist.txt); do ./run2.sh commit IcebergCommit $c; done
```

- `tla/commit/IcebergCommit.tla` the commit protocol: pointer, snapshot tree, sequence numbers, data and
  equality delete files, the retry loop, both conflict validations, atomic and non-atomic catalogs, lost
  commit answers.
- `tla/commit/configs.py` one config per claim. `wit_*` show an interleaving is reachable by violating an
  invariant on purpose; `*_fix_*` check the guarded version.
- `tla/commit/results.txt` the current verdicts, 15 configs, under 15 seconds for the lot.
- `findings/iceberg-commit-tla.md` the four findings with their traces, and the caveats.

TLC comes from `redis-lean/tla/tools/tla2tools.jar` and Java 21 from homebrew; both paths are in `run2.sh`.

## Next

1. A second model with the object store: crash before the pointer move, truncated manifest writes, and
   whether orphan file cleanup can be made safe without an in-flight commit registry in the catalog.
2. A log-to-table model for streaming ingestion: offsets in the snapshot summary, writer fencing after
   failover, no duplicates and no gaps. That is the conformance contract for Tableflow, the Flink sink,
   Kafka Connect and the Camel adapter.
3. The DST harness, reusing `camel-test-dst`: Iceberg's `FileIO` and `Catalog` are already interfaces, so
   the fakes replace per-component worlds. Iceberg has no thread pool seam, so either contribute one or
   reuse the clock rewriting agent.
