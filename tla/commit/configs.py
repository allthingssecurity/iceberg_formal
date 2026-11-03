# Iceberg commit protocol: one config per claim.
# Naming: wit_* shows an interleaving is reachable (invariant violated on purpose),
#         *_<invariant> checks one real invariant, *_fix_* checks the guarded version.
BASE = dict(
    OpA='"delete"', OpB='"rewrite"',
    PayA="{1}", PayB="{}",
    InitRows="{1, 2}",
    CATALOG='"cas"',
    ISOLATION='"snapshot"',
    REWRITE_KEEPS_SEQ="FALSE",
    VALIDATE_REWRITE_DELETES="FALSE",
    LOSE_ACK="FALSE",
    IDEMPOTENCY_TOKEN="FALSE",
    MaxRetry="1", MaxSnap="4", MaxSeq="5",
)

ALL  = ["NoLostCommit", "NoDoubleApply", "ReadStability", "Serializable", "DeleteHolds"]
APP  = dict(OpA='"append"', OpB='"append"', PayA="{1}", PayB="{2}", InitRows="{}")
DEL  = dict(OpA='"delete"', OpB='"append"', PayA="{1, 2}", PayB="{2}", InitRows="{1}")
BIG  = dict(MaxSnap="6", MaxSeq="7")

C = {
 # -- the interleavings are reachable at all
 "wit_both":        ({}, ["W_BothCommit"], []),
 "wit_retry":       ({}, ["W_Retry"], []),

 # -- compaction versus a concurrent delete
 "rw_serializable": ({}, ["Serializable"], []),
 "rw_deleteholds":  ({}, ["DeleteHolds"], []),
 "rw_fix_seq":      ({"REWRITE_KEEPS_SEQ": "TRUE"}, ALL, ["Termination"]),
 "rw_fix_validate": ({"VALIDATE_REWRITE_DELETES": "TRUE"}, ALL, ["Termination"]),

 # -- equality delete versus a concurrent append
 "da_deleteholds":  (dict(DEL), ["DeleteHolds"], []),
 "da_serializable": (dict(DEL), ["Serializable"], ["Termination"]),
 "da_wit_done":     (dict(DEL), ["W_Done"], []),
 "da_iso_serial":   (dict(DEL, ISOLATION='"serializable"'), ["DeleteHolds"], []),

 # -- catalog atomicity
 "cat_rename":      (dict(APP, CATALOG='"rename"'), ["NoLostCommit"], []),
 "cat_cas":         (dict(APP), ALL, ["Termination"]),

 # -- a commit whose answer was lost
 "unk_double":      (dict(APP, **BIG, LOSE_ACK="TRUE"), ["NoDoubleApply"], []),
 "unk_fix_token":   (dict(APP, **BIG, LOSE_ACK="TRUE", IDEMPOTENCY_TOKEN="TRUE"), ALL, ["Termination"]),

 # -- everything guarded, all invariants together
 "all_fix":         ({"REWRITE_KEEPS_SEQ": "TRUE", "VALIDATE_REWRITE_DELETES": "TRUE",
                      "IDEMPOTENCY_TOKEN": "TRUE", "LOSE_ACK": "TRUE",
                      "MaxSnap": "6", "MaxSeq": "7"}, ALL, ["Termination"]),
}
