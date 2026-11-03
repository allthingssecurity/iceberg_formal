# Kafka Connect sink exactly-once model (KcSink.tla). One config per claim.
# wit_* : the invariant is a non-vacuity witness and MUST be violated.
# cal*  : calibration (pre-fix must violate, fixed must pass).
BASE = dict(
    Tasks="{1, 2}", NP="2", MaxOff="2", MaxCommits="2", MaxCtl="7",
    MaxCrash="0", MaxMoves="2", MaxRewind="0", MaxFail="0",
    CLOSE='"fixed"', SEEKNULL='"earliest"', CRESET='"earliest"', REPLAY='"max"', FAIL='"retry"',
)
SAFE = ["TypeOK", "NoLoss", "NoDup"]
REL_112 = dict(SEEKNULL='"skip"', CRESET='"latest"', REPLAY='"max"', FAIL='"terminate"')

C = {
 # -- calibration 1: #12372 (1.8.1 open/close vs 1.9.0), other knobs at their safe values
 "cal1_prefix":       (dict(CLOSE='"prefix"'), SAFE, []),
 "cal1_fixed":        ({}, SAFE, []),
 # -- calibration 2: #16282/#17340 eager rewind of the coordinator (1.11.0 put vs 1.12.0 max)
 "cal2_put_rewind":   (dict(REPLAY='"put"', MaxRewind="1"), SAFE, []),
 "cal2_max_rewind":   (dict(MaxRewind="1"), SAFE, []),
 # -- F1: close() does not rewind a retained partition that has no committed offset
 "f1_seeknull_skip":  (dict(SEEKNULL='"skip"'), SAFE, []),
 # -- F2: coordinator group without checkpoint + auto.offset.reset=latest
 "f2_latest_crash":   (dict(CRESET='"latest"', MaxCrash="1"), SAFE, []),
 "f2_latest_move":    (dict(CRESET='"latest"'), SAFE, []),
 "f2_latest_fail":    (dict(CRESET='"latest"', FAIL='"terminate"', MaxFail="1", MaxCrash="1", MaxMoves="1"), SAFE, []),
 "f2_earliest_fail":  (dict(FAIL='"terminate"', MaxFail="1", MaxCrash="1", MaxMoves="1"), SAFE, []),
 # -- released 1.12.0 behaviour, everything on
 "rel112_all":        (dict(REL_112, MaxCrash="1", MaxRewind="1", MaxFail="1"), SAFE, []),
 # -- 1.12.0 + both candidate fixes (SEEKNULL=earliest, CRESET=earliest), everything on
 # (MaxMoves=2 with everything on exceeded 117M distinct states / 6 GB without finishing)
 "fix112_all":        (dict(FAIL='"terminate"', MaxCrash="1", MaxRewind="1", MaxFail="1", MaxMoves="1"), SAFE, []),
 "fix112_all_retry":  (dict(MaxCrash="1", MaxRewind="1", MaxFail="1", MaxMoves="1"), SAFE, []),
 # -- non-vacuity of the passing configs
 "wit_fix_commits":   (dict(FAIL='"terminate"', MaxCrash="1", MaxRewind="1", MaxFail="1", MaxMoves="1"), ["W_AllCommitted"], []),
 "wit_fix_rewinds":   (dict(MaxRewind="1"), ["W_Rewound"], []),
 # -- unknown knob value must be rejected by ASSUME (expected: TLC error)
 "neg_unknown_close": (dict(CLOSE='"bogus"'), SAFE, []),
}


def get(name):
    if name not in C:
        raise KeyError(f"unknown kc config {name!r}; known: {sorted(C)}")
    return C[name]
