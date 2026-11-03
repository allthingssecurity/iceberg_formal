/*
 * Iceberg cross-system commit: the object store and the catalog as separate
 * systems, writers that can die mid-write, and post-commit manifest cleanup.
 *
 * This is the half the TLA+ model (tla/commit/IcebergCommit.tla) left out.
 * Promela is used here rather than TLA+ because the interesting states are
 * "a file exists but is truncated" and "the process died between two I/O
 * calls", which are natural as process death and are awkward as TLA+ actions.
 *
 * Code anchors (apache/iceberg main @ 9fffeb63b16):
 *   SnapshotProducer.java:498   .onlyRetryOn(CommitFailedException.class)
 *   SnapshotProducer.java:538   CommitStateUnknownException rethrown, no cleanup
 *   SnapshotProducer.java:557-558  cleanUncommitted(saved.allManifests(io))
 *   SnapshotProducer.java:772-785  deleteUncommitted: delete every manifest of
 *                               this operation that `committed` does not hold,
 *                               where `committed` is the NEW snapshot's
 *                               manifests only, never other live snapshots'
 *   MergingSnapshotProducer.java:1120-1132 cleanUncommittedAppends: the
 *                               ownership argument that makes that safe
 *   MergingSnapshotProducer.java:323-338   add(ManifestFile): a manifest that
 *                               can inherit the snapshot id is kept as the
 *                               caller's own file in appendManifests; one that
 *                               cannot is COPIED into rewrittenAppendManifests
 *
 * Defines:
 *   REUSE                 a writer may append a manifest that a live snapshot
 *                         already references (appendManifest with a file that
 *                         was appended before). Off by default.
 *   CHECK_ALL_SNAPSHOTS   candidate fix: deleteUncommitted skips a manifest
 *                         that any live snapshot still references.
 *   NOCRASH               disable process death, to isolate the cleanup race.
 */

#define NF 6            /* object store slots  */
#define NS 4            /* snapshot slots      */

#define ABSENT   0
#define PARTIAL  1      /* created, then the writer died mid-write */
#define COMPLETE 2

byte fstate[NF];        /* the object store */
bool refs[NS*NF];       /* refs[s*NF+f]: snapshot s references file f */
byte parent[NS];
bool live[NS];
byte ptr;               /* the catalog pointer */
byte nsnap;
byte nfile;

bool ok[2];             /* the writer saw its commit succeed */
byte mine[2];
byte landed[2];

/* With an atomic CAS the parent of every new snapshot is the pointer it
   replaced, so the snapshot log is one chain and every committed snapshot is
   an ancestor of ptr. live[] is therefore exactly the reachable set, which is
   asserted in Chain() rather than assumed. */

inline copy_refs(src, dst) {
    ci = 0;
    do
    :: ci < NF -> refs[dst*NF+ci] = refs[src*NF+ci]; ci++
    :: else -> break
    od
}

/* PROPERTY 1: nothing a live snapshot references is ever missing or truncated */
inline assert_refs_complete(s) {
    ci = 0;
    do
    :: ci < NF ->
        assert(!refs[s*NF+ci] || fstate[ci] == COMPLETE);
        ci++
    :: else -> break
    od
}

/* PROPERTY 2: cleanup never deletes a file a live snapshot still references */
inline do_delete(f) {
    cj = 0;
    do
    :: cj < NS ->
        if
        :: live[cj] -> assert(!refs[cj*NF+f])
        :: else -> skip
        fi;
        cj++
    :: else -> break
    od;
    fstate[f] = ABSENT
}

inline referenced_anywhere(f, out) {
    out = 0; cj = 0;
    do
    :: cj < NS ->
        if
        :: live[cj] && refs[cj*NF+f] -> out = 1
        :: else -> skip
        fi;
        cj++
    :: else -> break
    od
}

proctype Writer(byte id) {
    byte f1;            /* the manifest this operation writes or appends */
    byte merged = 255;  /* set when the commit references a merged manifest instead */
    byte s;
    byte attempts = 0;
    byte ci, cj, r;

    /* ---- choose the manifest this operation contributes ---- */
    if
    :: atomic { f1 = nfile; nfile++ };          /* a manifest we write ourselves */
       if
       :: fstate[f1] = COMPLETE                 /* the write returned */
       :: fstate[f1] = PARTIAL; goto dead       /* died mid-write: truncated file left */
       :: goto dead                             /* died before any bytes */
       fi
#ifdef REUSE
    :: f1 = 0                                   /* appendManifest of a file a live
                                                   snapshot already references */
#endif
    fi;

commit:
    attempts++;
    if
    :: nsnap < NS && nfile < NF ->
       /* ---- the catalog accepted the compare-and-set ---- */
       atomic {
           s = nsnap; nsnap++;
           live[s] = true; parent[s] = ptr;
           copy_refs(ptr, s);
           /* the new snapshot references our manifest, or a merged manifest
              that replaces it (manifest merging), leaving ours uncommitted */
           if
           :: refs[s*NF+f1] = true
           :: merged = nfile; nfile++;
              fstate[merged] = COMPLETE;
              refs[s*NF+merged] = true;
              refs[s*NF+f1] = false
           fi;
           ptr = s;
           mine[id] = s; landed[id]++;
           assert_refs_complete(s)
       };
       if
       :: ok[id] = true                          /* the answer arrived */
       :: skip                                   /* CommitStateUnknownException:
                                                    landed, but we never learned.
                                                    SnapshotProducer:538 rethrows
                                                    and cleans nothing. */
       fi
    :: /* ---- CommitFailedException: another writer moved the pointer ---- */
       if
       :: attempts < 2 -> goto commit
       :: else -> goto failed
       fi
    fi;

    /* ---- post-commit cleanup, SnapshotProducer:557-558 + 772-785 ----
       delete the manifests of this operation that the SAVED snapshot does not
       reference. `committed` is that one snapshot's manifests. */
    if
    :: merged != 255 ->
#ifdef CHECK_ALL_SNAPSHOTS
       referenced_anywhere(f1, r);
       if
       :: r == 1 -> skip
       :: else -> atomic { do_delete(f1) }
       fi
#else
       atomic { do_delete(f1) }
#endif
    :: else -> skip
    fi;
    goto out;

failed:
    /* cleanAll, SnapshotProducer:612-617. Only reached on a CleanableFailure,
       so the commit definitively did not land and our own files are garbage. */
    if
    :: merged == 255 && fstate[f1] != ABSENT -> atomic { do_delete(f1) }
    :: else -> skip
    fi;
    goto out;

dead:
    /* the process died. Nothing was committed, so whatever is in the store is
       garbage and not corruption: that is the property to confirm, not assume. */
    skip;

out:
    skip
}

#ifdef ORPHAN
/*
 * DeleteOrphanFiles: list the store, compute the files the table references,
 * delete the difference. The only guard in the real action is a modification
 * time filter (DeleteOrphanFilesSparkAction.java:129, default 3 days) applied
 * during listing, compared against the DRIVER's clock. Define SHORT_RETENTION
 * to model either a short interval or a store clock behind the driver's, which
 * is what makes a freshly written file look old enough to collect.
 */
proctype Cleaner() {
    byte ci, cj, f, r;
    f = 0;
    do
    :: f < NF ->
        /* candidate: present on disk, and old enough to pass the mtime filter */
        if
        :: fstate[f] != ABSENT ->
#ifdef SHORT_RETENTION
            referenced_anywhere(f, r);
            if
            :: r == 0 -> atomic { do_delete(f) }
            :: else -> skip
            fi
#else
            /* with the default interval a file written by an in-flight commit
               is far too new to be listed, so only file 0 is ever a candidate */
            if
            :: f == 0 ->
               referenced_anywhere(f, r);
               if
               :: r == 0 -> atomic { do_delete(f) }
               :: else -> skip
               fi
            :: else -> skip
            fi
#endif
        :: else -> skip
        fi;
        f++
    :: else -> break
    od
}
#endif

/* every live snapshot still reads correctly, in every reachable state */
active proctype Chain() {
    byte ci, cj, c;
    do
    :: atomic {
        cj = 0;
        do
        :: cj < NS ->
            if
            :: live[cj] -> assert_refs_complete(cj)
            :: else -> skip
            fi;
            cj++
        :: else -> break
        od;
        /* the log is one chain: every live snapshot is an ancestor of ptr */
        c = ptr;
        do
        :: c == 0 -> break
        :: c != 0 -> assert(live[c]); c = parent[c]
        od
       }
    od
}

init {
    byte i;
    d_step {
        i = 0; do :: i < NF -> fstate[i] = ABSENT; i++ :: else -> break od;
        i = 0; do :: i < NS*NF -> refs[i] = false; i++ :: else -> break od;
        i = 0; do :: i < NS -> live[i] = false; parent[i] = 0; i++ :: else -> break od;
        i = 0; do :: i < 2 -> ok[i] = false; mine[i] = 0; landed[i] = 0; i++ :: else -> break od;
        /* the table starts with one committed manifest, file 0 */
        fstate[0] = COMPLETE; refs[0] = true; live[0] = true;
        ptr = 0; nsnap = 1; nfile = 1
    };
#ifdef ORPHAN
    atomic { run Writer(0); run Cleaner() }
#else
    atomic { run Writer(0); run Writer(1) }
#endif
}
