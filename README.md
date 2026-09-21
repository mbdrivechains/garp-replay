# garp_replay — block-driven BTC → ECX replay bridge

A small, stdlib-only Python daemon that keeps the eCash (ECX) mempool supplied with every
transaction Bitcoin confirms after the GARP fork, working from **Bitcoin's chain** rather than
Bitcoin's gossip. It is the block-driven, persistent, idempotent counterpart to the one-shot
`-bitcoinpeer` gossip tap: whatever ECX refuses at first sight for a *transient* reason is kept
and offered again after every ECX block, until it is accepted, present, or provably dead.

Status: **running in production on eCash betanet since 2026-09-20**, where it is currently the
only source of replayed Bitcoin transactions: of ~41,650 transactions in betanet's mempool, 41,632
were placed by this tool, and new Bitcoin blocks arrive with 0–92 of their ~3,500 transactions
already present. Over 970,000 transactions restored so far. Validated before that on a regtest pair
(vanilla Core 29 + the eCash betanet binary) with the 22 scenarios in `tests/`.

## Why it exists

eCash replays Bitcoin's transactions, but the seed's block feed drops any transaction the eCash
mempool refuses for a *transient* reason — most often `too-large-cluster`, because a Bitcoin block's
worth of chained transactions arrives all at once and eCash caps a connected group at 64. A dropped
transaction is never re-offered, and every later Bitcoin transaction spending its outputs is then
missing its inputs forever. On betanet that took replay from 99.2% of Bitcoin's transactions down to
~52% within hours of the first difficulty retarget, and 122,741 transactions were absent purely
because an ancestor was absent.

The one-line fix for the classification is filed as
[ecash-com/bitcoin#1](https://github.com/ecash-com/bitcoin/pull/1). It stops the routine losses. It
does not survive a sustained deficit: the in-node queue is capped at 50,000, expires after 24 h and
is lost on restart, and eCash's block space is only ~61–78% of Bitcoin's output when hashrate dips.
This tool keeps the queue durable and unbounded on disk instead, so a backlog is delayed rather than
destroyed, and can re-walk the chain from the fork to repair damage already done.

---

## Quick start

You need two nodes on the same machine and Python ≥ 3.11. Nothing else.

**1. A Bitcoin node.** Any Core 29+, pruned is fine (blocks are read over RPC, no `txindex`
needed). In `bitcoin.conf`:

```
server=1
rpcuser=user
rpcpassword=CHANGEME
```

**2. An eCash node**, synced and on the network, with a transaction index. In `ecash.conf`:

```
server=1
txindex=1
rpcuser=user
rpcpassword=CHANGEME
```

**3. Configure the bridge.** `cp garp_replay.example.toml garp_replay.toml`, then set the two RPC
URLs and the network:

```toml
network = "mainnet"          # or betanet / alphanet / regtest
[btc]
rpc_url = "http://user:CHANGEME@127.0.0.1:8332"
[ecx]
rpc_url = "http://user:CHANGEME@127.0.0.1:8532"
[state]
path = "/var/lib/garp-replay/state.sqlite"
```

**4. Dry run first** — this submits nothing, it only reports what it would do:

```bash
python3 garp_replay.py --config garp_replay.toml --mode live
```

You should see one line per Bitcoin block: how many of its transactions are already on eCash, how
many would be injected, how many are dead. If it refuses to start it will say why (node syncing,
`txindex` behind, chains disagreeing) — fix that first.

**5. Go live.** `--live-send` is the only thing that makes it submit, and it cannot be set from the
config file:

```bash
python3 garp_replay.py --config garp_replay.toml --mode live --live-send
```

**6. Keep it running.** A user systemd unit is enough:

```ini
[Unit]
Description=garp-replay BTC->ECX replay bridge
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 /opt/garp-replay/garp_replay.py --config /etc/garp-replay.toml --mode live --live-send
Restart=always
RestartSec=30

[Install]
WantedBy=default.target
```

### Is it working?

The per-block log line is the answer. `present` is how many of that Bitcoin block's transactions
reached eCash by any other route before the bridge got there; `injected` is how many the bridge
placed. On a chain where nothing else is feeding replay, `present` is near zero and `injected` is
most of the block. `status.json` carries the same counters plus the queue depth.

To repair a chain that has already fallen behind, run `--mode backfill` once from the fork height
(`--start-height`), then switch to `--mode live`. Multiple operators can run this at the same time:
submitting a transaction someone else already relayed is a no-op.

### At a fork

Start it *before* the fork height is reached, in `--mode live`. It idles until the first post-fork
Bitcoin block exists, then keeps eCash current from that block onward, so no gap ever forms. A gap
of even a few blocks compounds: on betanet, ~13 hours of dropped transactions took more than a day
to repair.

---

## What it does

For each BTC block from the fork height (or the saved cursor) to the tip, in block order:

| step | action | RPC cost |
|---|---|---|
| 1 | any input spends a txid in the **dead-cache** (post-fork BTC coinbase, or a tx already found dead) → `dead` | 0 |
| 2 | any input spends a txid in the **retry queue** → `retry:child-of-retry` (queued behind its parent; or a `submitpackage` if the parent is a package-kind parent) | 0 |
| 3 | **presence** on ECX via txindex (`getrawtransaction`, batched ≤100) → `present:confirmed` / `present:mempool` (someone else, or an earlier run, relayed it — the "aware of others" signal) | 1/100 |
| 3b | feerate below ECX's dynamic `mempoolminfee` → `retry:minfee` without submitting | 0 |
| 4 | **submit**: `sendrawtransaction(hex, maxfeerate=0, maxburnamount=21e6)`, or `testmempoolaccept` in a dry run; the reject string is classified (table below) | 1 |

The retry queue is swept in (BTC height, block index) order after every ECX block and at least
every `retry_interval_secs`. Every transaction that is in the ECX mempool because of us **or anyone
else** (`injected`, and `present:mempool`) is tracked in `pending`; after `verify_after_secs` those
rows are presence-checked and re-queued if the ECX mempool dropped them (eviction, expiry, node
restart without `persistmempool`). The same record decides the fate of a child whose parent is
missing on ECX: a parent with a `pending` row was *dropped* and is re-queued from its BTC block on
the spot (child → `retry:child-of-retry`); a parent with no record at all was never accepted by ECX
and the child is `dead:ancestor`. Absence alone is never cached as dead.

A node that comes back having lost its mempool *entirely* is a different size of problem:
`mempool.dat` is only written on a clean shutdown, so a power cut, an OOM kill or a crash drops
everything injected and not yet mined, and with it every later transaction spending one of those
outputs. Working through that with the age gate and one batch per ECX block would take hours. So
the whole `pending` table is reconciled against `getrawmempool` in a single pass — at startup, and
again whenever ECX's `uptime` goes backwards — with a batched presence check of only the rows the
mempool no longer holds. What was mined is retired; what is genuinely gone is re-queued from its
Bitcoin block.

### Classifier (exact strings of Core v31 / `ecash-com/alphanet`, all verified on regtest)

| ECX says | class | meaning |
|---|---|---|
| accepted | `injected` | now in the ECX mempool (→ `pending`) |
| `txn-already-in-mempool`, `txn-same-nonwitness-data-in-mempool`, `txn-already-known`, `Transaction outputs already in utxo set` | `present` | no-op |
| `missing-inputs` / `bad-txns-inputs-missingorspent` | resolved per input: parent present on ECX but outpoint gone → **`conflict:split`**; parent absent and known dead, or absent with no `pending` record → **`dead:ancestor`**; parent absent but in `pending` (dropped by ECX) → parent re-queued as `retry:evicted`, child `retry:child-of-retry`; parent in the retry queue → `retry:child-of-retry` | |
| `non-final`, `non-BIP68-final`, `TRUC-violation`, `too-large-cluster`, `too-long-mempool-chain`, `mempool full`, `mempool min fee not met`, `bad-txns-premature-spend-of-coinbase`, `too many potential replacements`, `replacement-adds-unconfirmed` | `retry:<tag>` | transient on a slower chain; queued |
| `min relay fee not met`, `dust`, `missing-ephemeral-spends` | `package:<tag>` | the parent cannot stand alone; when its child arrives, `submitpackage([parents…, child])` |
| `insufficient fee`, `replacement-failed` (RBF loses to an ECX mempool variant, incl. v31's feerate-diagram check), `txn-mempool-conflict` | `conflict:<tag>` | counted, never fought |
| anything else | `policy:<tag>` | recorded; re-offered until `max_attempts`, then dropped (only policy rejects are ever given up; transient kinds wait for ECX blockspace indefinitely) |

`txn-mempool-conflict`, `too-long-mempool-chain`, `missing-ephemeral-spends` and
`package-not-child-with-unconfirmed-parents` do not occur in the v31 tree; they are kept as
harmless aliases. Every other string has been produced on regtest by the alphanet binary.
JSON-RPC errors that describe the node rather than the transaction (`-28` warming up, `-10`,
`-20`, protocol errors) are never classified: they are retried at the transport layer and, in
`--mode live`, waited out (see below).

`present` is checked **before** submitting on purpose: a long-confirmed ECX transaction whose
outputs have all been spent comes back as `missing-inputs` from ATMP, and would be misclassified
as dead without txindex. This is why the ECX node needs `txindex=1`.

### Modes

| flag | behaviour |
|---|---|
| *(default)* | **dry run**: `testmempoolaccept`, one tx per call. Nothing is submitted. A child whose parent *would have been* injected is reported as `retry:dry-run-parent` (it cannot be evaluated without the parent) and is not queued. Would-be injections are remembered in `pending` so that a restart keeps reporting their children as `dry-run-parent`, not `dead:ancestor`. |
| `--simulate` | dry run plus a virtual overlay: outputs of would-be-injected txs count as present, virtual double-spends are detected, so descendants of a recoverable root count as recoverable. This is the measurement tool ("how much of block X is recoverable?"). |

**Dry-run caveat for packages (CPFP, zero-fee parents, ephemeral dust, TRUC pairs).** Core's
`testmempoolaccept` judges every package member on its own fee (the test path runs with
`package_feerates=false`, verified on ECX v31 and vanilla 29), so a zero-fee parent + CPFP child can
*never* pass a dry-run package test although `submitpackage` accepts it live. Both dry modes
therefore fall back to an **estimate** when the parent alone is rejected for `min relay fee` / `dust`:
if the package feerate computed from the BTC node's `getblock … 2` fee data clears ECX's
`minrelaytxfee`, the package is counted as injected (`status.json: package_estimated` says how many).
The child's scripts are not evaluated in that case — the same caveat as any descendant of a would-be
injection. If the BTC node has no fee data (pruned block whose undo data is gone) the package stays
`package` + `retry:child-of-retry` in the dry run, and the number is biased low on exactly the
transaction shapes mainnet is full of; check `package_estimated` before reading `--simulate` output
as "the recoverable share".
| `--live-send` | actually submit (`sendrawtransaction` / `submitpackage`). **Only the command line can enable this; a TOML file cannot.** |
| `--mode backfill` | cursor → BTC tip, one final retry sweep, exit. |
| `--mode live` | backfill, then follow BTC with `waitfornewblock` (Core 29 and 31), following ECX blocks for sweeps and pending verification. A node restart, warm-up (`-28`) or outage does not kill the daemon: it logs, waits (poll interval doubling to 5 min), re-validates both nodes and re-walks whatever was interrupted (every state write is transactional). `SIGTERM` finishes the current block and exits. |

---

## Running it

Requirements: Python ≥ 3.11 (for `tomllib`), a Bitcoin Core node (29+, pruned is fine, no
txindex needed: blocks are read with `getblock … 2`), and an ECX node with `txindex=1`.

```bash
cp garp_replay.example.toml garp_replay.toml   # edit RPC endpoints, state path, network

# 1. first run on any network: dry run on the live tip
python3 garp_replay.py --config garp_replay.toml --mode live

# 2. the recoverable share since the fork, without touching ECX
python3 garp_replay.py --config garp_replay.toml --simulate --mode backfill --state sim.sqlite

# 3. live
python3 garp_replay.py --config garp_replay.toml --mode live --live-send
```

Most settings have a CLI flag (`--btc-url`, `--ecx-url`, `--state`, `--start-height`,
`--mempool-fraction`, …; see `--help`); `require_txindex`, `submit_delay_ms`, `presence_batch`,
`verify_batch` and `rolling_blocks` are TOML-only. Network presets: `alphanet` (fork 963648),
`betanet` (967680), `mainnet` (973728); `regtest` requires `--fork-height`.

Startup refuses (backfill) or waits for (live) a node that is not usable: ECX in initial block
download, ECX or BTC tip below the last shared block, ECX `txindex` not synced or ahead of the
chainstate, BTC and ECX disagreeing on the last shared block. Walking blocks against an ECX node
that is syncing, reindexing or rolled back would record everything as `conflict`/`dead`, so it is
never attempted. If the BTC tip drops below the cursor (an invalidated tip, a node being restored)
the bridge waits rather than rolling back on the strength of a missing block.

State (`state.sqlite`, WAL) holds the cursor (last 200 heights+hashes for reorg detection),
per-block stats, the retry queue (with the tx hex, so sweeps never re-read BTC blocks), and
`pending`. The cursor is advanced in the same transaction as the block's stats and queue rows, so
a crash mid-block simply re-walks that block on restart (everything already submitted shows up as
`present`). `--reset-cursor` forgets all of it.

Pacing: before every block and every 500 submissions the bridge reads `getmempoolinfo`; while
ECX mempool usage exceeds `mempool_fraction × maxmempool` (default 0.85) it pauses. Transactions
below the dynamic `mempoolminfee` are queued without an RPC. Nothing is ever prioritised or evicted.

ECX follower: new ECX blocks are walked back from the tip to the last seen tip, bounded by height
(new blocks + 6 when the last seen tip was reorged away — never the whole chain); full blocks are
only read while `pending` is non-empty. A transaction confirmed in a reorged-out ECX block is not
re-tracked (it is back in the ECX mempool or ECX's own relay fetches it again). If the ECX tip goes
backwards by more than 6 blocks the node is re-validated as at startup.

### Output

One log line per BTC block:

```
block 965972 tx=4020 present=591 injected=9 dead=3420 (ancestor 3405, coinbase 12) retry=6 (bip68 2, cluster 1, truc 3) package=0 policy=0 conflict=3 (split 3) rpc=612 (ecx 611) t=1.4s coverage=14.9% (144-blk 14.2%)
```

`status.json` (rewritten atomically after every block): mode flags, cursor, both tips, ECX
mempool size/usage/minfee, cumulative totals per class, rolling 144-block coverage, retry queue
size with per-tag breakdown, pending count (in a dry run: remembered would-be injections),
`package_estimated`, cumulative sweep results, per-method RPC counts and seconds for both nodes,
and the last 20 block rows. `--print-status` prints it.

### Confirmed-set export (`--confirmed-file PATH`)

For a miner-side template filter ("do not mine a BTC-gossiped tx before BTC confirms it") the
bridge can publish the set of txids BTC has confirmed. After every processed block it atomically
rewrites `PATH` (write temp + rename) as:

```
# btc_height=<h> btc_hash=<hash> updated=<unix time>
<txid>
<txid>
…
```

one hex txid per line for **every non-coinbase transaction in the last `--confirmed-window`
BTC blocks** (default 288 ≈ 2 days). The window is restored from the BTC node on restart. Note the
size: at mainnet density (≈3,500 tx/block) 288 blocks is ≈1 M lines ≈ 65 MB; use a smaller window
(36 blocks ≈ 8 MB) if the consumer only needs recent confirmations.

---

## Coexistence etiquette ("aware of others")

Several bridges — L2L's gossip tap, this one, anyone else's — can run at once without
coordination:

* **Only BTC-confirmed content is offered**, in block order. The bridge cannot introduce a
  transaction Bitcoin did not confirm, and it forwards nothing from the BTC mempool.
* **Idempotent.** `txn-already-in-mempool` / `txn-already-known` are no-ops at the node and on
  the network (peers dedupe by wtxid). Re-running a block, restarting, or running N instances
  with separate state files produces the same ECX mempool (scenario T10).
* **It never fights.** No `prioritisetransaction`, no eviction, no re-submission of a transaction
  ECX has replaced or mined differently: `conflict` is counted, not contested (T5). A BTC-confirmed
  replacement will RBF-replace a stale variant in the ECX mempool only because ECX's own policy
  accepts it — the same thing any peer relaying the confirmed version would achieve.
* **Load stays local.** One serialised RPC connection per node, presence checks batched; the
  design assumes both nodes are yours. Do not point it at someone else's public RPC.
* `present` per block tells you how much others are already covering; if it sits near 100 %
  your instance is redundant, which is harmless.
* If you want to lag one block behind BTC (never re-walk on 1-block reorgs), set
  `confirmations_required = 2`.

## What it does NOT fix

Read the assessment before expecting replay percentages to move. Measured on alphanet, the
missing mass traces to three root classes, and this bridge addresses only the smallest:

| root class | share (day 16) | fixable by this bridge? |
|---|---|---|
| **RBF splits**: ECX mined the first-seen version V of a payment before BTC confirmed the fee-bumped replacement X; X and every BTC descendant of X are invalid on ECX forever | ~66 % (100 % at fork+12) | **No.** Counted as `conflict:split`. Needs an ingestion/template policy on the ECX side (confirm BTC-gossiped txs only after BTC does) — the `--confirmed-file` export exists to feed such a filter. |
| **post-fork coinbase taint**: spends of BTC coinbases mined after the fork, and everything downstream | ~31 %, monotonic | **No**, inherent to GARP. Counted as `dead:coinbase` / `dead:ancestor` with zero RPC. |
| **first-sight rejects and relay misses**: `non-BIP68-final`, `TRUC-violation`, cluster limits, mempool full, orphaned children, bridge outages | ~3 % (≈0.2 %/block, compounding) | **Yes** — this is the retry queue's job. |

Also outside its reach: transactions that are non-standard under ECX policy (mined out-of-band
on BTC), zero-fee transactions with no CPFP child on BTC (`package:minrelay` stays queued), and
the 220 repurpose transactions' inputs. `policy` counts the first class so its size is known.

## Tests

`tests/test_scenarios.py` starts a vanilla Core 29 regtest node and an eCash alphanet-binary
regtest node (same genesis, `-listen=0 -connect=0`), feeds both to height 201 with a fan-out
transaction, imports the same wallet descriptors into both, forks them (ECX mines ahead, as on
the real networks), and drives each scenario through the BTC side:

| | scenario | asserts |
|---|---|---|
| T1 | 50 plain txs in one BTC block | all in the ECX mempool; rerun 0 submits; fresh second instance present=50 |
| T2 | 30-deep chain over 3 BTC blocks, ECX `-limitclustercount=10`, not mining | tail `retry:cluster` + `child-of-retry` (0 ECX RPC); drains one ECX block at a time |
| T3 | post-fork BTC coinbase spend + 3 descendants | `dead:coinbase` 1, `dead:ancestor` 3, zero ECX RPC |
| T5 | RBF: A in ECX mempool, BTC confirms A′ / ECX mined B first, BTC confirms B′ | A′ replaces A; B′ → `conflict:split`, one attempt, never re-submitted |
| T6 | BTC reorg with a different tx set | fork point found, cursor rolled back, re-walk, stale retry row gone |
| T7 | v3 zero-fee parent + CPFP child | `package:minrelay`, child triggers `submitpackage`, both in ECX mempool |
| T8 | CSV child (nSequence=10) | `retry:bip68` until ECX has mined 9 blocks past the parent |
| T10 | `kill -9` after 100 submits, restart; two instances concurrently | cursor untouched, full coverage, no duplicates, identical coverage |
| T12 | default dry run | ECX mempool unchanged; counts identical to the live run |
| T13 | `--confirmed-file` | header + exactly the last-N-blocks txids; restored after restart |
| T14 | dry run vs `--simulate` on a parent/child | `retry:dry-run-parent` vs virtual injection |
| T15 | `--mode live` | new BTC block picked up via `waitfornewblock`; ECX block clears pending |
| T16 | ECX restarted without `persistmempool` under a live bridge; BTC then confirms two children of the lost parent | bridge survives the restart; parent re-queued `evicted`, children never `dead`; all three delivered |
| T17 | 12-deep chain, cluster limit 10, `--max-attempts 2`, four runs | transient kinds never given up; injected once ECX mines |
| T18 | ECX node in IBD / below the last shared block | backfill refuses (nothing recorded); live waits, then proceeds |
| T19 | zero-fee v2 parent + CPFP child + sibling (out-of-band on BTC) | `--simulate` = live (injected 2, `package_estimated` 1); plain dry run 1 + `dry-run-parent` |
| T20 | 500 plain + 21 dead txs in one block | one `getmempoolinfo` per block + one per 500 submits, none per dead tx |
| T21 | BTC tip below the cursor (invalidated tip) | waits, cursor kept; reorg handled once a replacement block exists |
| T22 | 1-block ECX reorg under a live bridge; ECX block between backfill runs | ≤ 8 headers walked, 0 full blocks while nothing is pending; `ecx_tip` persisted |
| T23 | v31 feerate-diagram RBF loss (`replacement-failed`) | `conflict:rbf-loss`, never re-submitted |
| T24 | unit stubs (no nodes) | batch error object → `RPCError`; `IncompleteRead` retried; sweep `PRESENT` dequeues |
| T25 | ECX loses its whole mempool (no `persistmempool`), under a live bridge and while it is down | restart detected via `uptime`; the injected set reconciled in one pass and re-injected in both cases |

```bash
python3 tests/test_scenarios.py          # all (~6 min, 20 node pairs sequentially)
python3 tests/test_scenarios.py T2 T5    # a subset
GARP_VANILLA_BIN=... GARP_ECX_BIN=... GARP_REGTEST_ROOT=...   # binary and datadir locations
```

Not covered by tests (see the assessment's caveats): a real network soak, `mempool full`
pacing under a sustained BTC backlog (T11 in the design), non-standard-on-ECX transactions (T9),
the 6-hour pending verifier itself (its re-queue helper is exercised by T16 through the
missing-parent path), and a BTC-side outage under a live bridge (the same transport code path as
the ECX restart in T16, but not driven).

Known limits of the "absent + no record = dead" rule: an instance started with `--start-height`
above the fork has no `pending` record for parents in the skipped blocks, so a parent injected by
someone else and later evicted makes its children `dead:ancestor` on that instance (another
instance that walked the block recovers them); a parent confirmed on ECX and then dropped by an ECX
reorg deeper than the follower's bound is likewise not re-tracked.

## License

MIT — Michael Blowes, 2026.
