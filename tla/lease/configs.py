# Lease-protected commits (r5 C-3/C-8 Hive, C-11/C-13 DynamoDB+Glue). One config per claim.
# Naming: wit_* shows a schedule is reachable (invariant violated on purpose),
#         <lock>_<claim> checks NoLostCommit, *_cas checks the guarded write (candidate fix).
BASE = dict(
    LOCK='"hms"', CAS="FALSE", ALLOW_PAUSE="TRUE",
    HB="1", T="2", LEASE="2", MaxTime="7", MaxRetry="1",
)
DYN = dict(LOCK='"dynamo"')
SAFE = ["TypeOK", "NoLostCommit"]

C = {
 # -- Hive MetastoreLock (calibrated against the real-HMS run in findings/r5-catalogs.md)
 "hms_wit_both":     ({}, ["W_BothSuccess"], []),
 "hms_wit_lapse":    ({}, ["W_LeaseLapse"], []),
 "hms_nopause":      ({"ALLOW_PAUSE": "FALSE"}, SAFE, []),
 "hms_pause":        ({}, SAFE, []),
 "hms_pause_cas":    ({"CAS": "TRUE"}, SAFE, []),
 # -- DynamoDbLockManager + GlueTableOperations (model only: no AWS here)
 "dyn_wit_both":     (dict(DYN), ["W_BothSuccess"], []),
 "dyn_wit_lapse":    (dict(DYN), ["W_LeaseLapse"], []),
 "dyn_nopause":      (dict(DYN, ALLOW_PAUSE="FALSE"), SAFE, []),
 "dyn_pause":        (dict(DYN), SAFE, []),
 "dyn_pause_cas":    (dict(DYN, CAS="TRUE"), SAFE, []),
 # -- non-vacuity of the no-pause controls: both writers still succeed, and no lease lapses
 "hms_nopause_wit_both": ({"ALLOW_PAUSE": "FALSE"}, ["W_BothSuccess"], []),
 "hms_nopause_nolapse":  ({"ALLOW_PAUSE": "FALSE"}, ["W_LeaseLapse"], []),
 "dyn_nopause_wit_both": (dict(DYN, ALLOW_PAUSE="FALSE"), ["W_BothSuccess"], []),
 "dyn_nopause_nolapse":  (dict(DYN, ALLOW_PAUSE="FALSE"), ["W_LeaseLapse"], []),
 # -- an unknown lock name must be rejected by the spec's ASSUME (expected: TLC error)
 "neg_unknown_lock": ({"LOCK": '"zookeeper"'}, SAFE, []),
}


def get(name):
    """Look a config up by name; an unknown name throws."""
    if name not in C:
        raise KeyError(f"unknown lease config {name!r}; known: {sorted(C)}")
    return C[name]
