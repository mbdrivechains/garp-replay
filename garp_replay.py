#!/usr/bin/env python3
"""
garp_replay — block-driven BTC → ECX transaction replay bridge (Python prototype).

Works from Bitcoin's *chain*, not its gossip: every non-coinbase transaction confirmed on BTC
since the fork is offered to the ECX node over RPC, in block order (parents before children by
consensus). Whatever ECX refuses for a transient reason (relative/absolute locks that have not
elapsed on the slower chain, cluster limits, a full mempool, TRUC ordering) goes into a durable
retry queue and is offered again after every ECX block. Whatever is provably dead (spends a
post-fork BTC coinbase, or an output ECX already spent differently) is counted and never fought.

Safety properties:
  * dry-run by default: nothing is submitted unless --live-send is given;
  * only BTC-*confirmed* transactions are ever offered, so the bridge cannot introduce a variant
    that BTC did not confirm;
  * idempotent: re-running a block, restarting, or running several instances changes nothing on
    ECX ("already known" is a no-op at the node and on the network);
  * it never evicts, never prioritises, never re-submits a transaction ECX has replaced or
    confirmed differently.

Stdlib only (urllib JSON-RPC, sqlite3, tomllib, argparse). See README.md for the operator guide.
"""
import argparse
import base64
import collections
import http.client
import json
import logging
import os
import random
import signal
import sqlite3
import sys
import time
import tomllib
import urllib.error
import urllib.request

__version__ = "0.1.1"
log = logging.getLogger("garp_replay")

# --------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------

NETWORKS = {
    # fork height = first BTC block whose transactions must be replayed (BTC and ECX share
    # every block below it). RPC ports are the conventional local defaults; override in TOML.
    "alphanet": {"fork_height": 963648, "btc_url": "http://127.0.0.1:8332", "ecx_url": "http://127.0.0.1:18303"},
    "betanet":  {"fork_height": 967680, "btc_url": "http://127.0.0.1:8332", "ecx_url": "http://127.0.0.1:18303"},
    "mainnet":  {"fork_height": 973728, "btc_url": "http://127.0.0.1:8332", "ecx_url": "http://127.0.0.1:18303"},
    "regtest":  {"fork_height": None,   "btc_url": "http://127.0.0.1:18443", "ecx_url": "http://127.0.0.1:18643"},
}

DEFAULTS = {
    "network": "alphanet",
    "fork_height": None,
    "btc": {"rpc_url": None, "cookie": None, "user": None, "password": None},
    "ecx": {"rpc_url": None, "cookie": None, "user": None, "password": None, "require_txindex": True},
    "run": {"mode": "live", "start_height": None, "confirmations_required": 1,
            "dry_run": True, "simulate": False, "max_attempts": 500},
    "pacing": {"mempool_fraction": 0.85, "pace_poll_secs": 60, "retry_interval_secs": 600,
               "verify_after_secs": 21600, "btc_poll_secs": 30, "submit_delay_ms": 0},
    "limits": {"dead_cache_entries": 2_000_000, "presence_batch": 100, "verify_batch": 2000},
    "state": {"path": "garp_replay.sqlite"},
    "report": {"status_file": "status.json", "confirmed_file": None, "confirmed_window": 288,
               "log_level": "info", "rolling_blocks": 144},
}

ECX_REORG_DEPTH = 6     # extra ECX blocks walked back when the last seen ECX tip is no longer in the chain
ECX_WALK_MAX = 2000     # hard cap on the ECX block walk per follow_ecx call
PACE_EVERY = 500        # submissions between two getmempoolinfo pacing checks

# CLI flag -> (section, key). Only flags actually given on the command line override the file.
CLI_MAP = {
    "network": (None, "network"), "fork_height": (None, "fork_height"),
    "btc_url": ("btc", "rpc_url"), "btc_cookie": ("btc", "cookie"), "btc_user": ("btc", "user"),
    "btc_password": ("btc", "password"),
    "ecx_url": ("ecx", "rpc_url"), "ecx_cookie": ("ecx", "cookie"), "ecx_user": ("ecx", "user"),
    "ecx_password": ("ecx", "password"),
    "mode": ("run", "mode"), "start_height": ("run", "start_height"),
    "confirmations_required": ("run", "confirmations_required"), "max_attempts": ("run", "max_attempts"),
    "mempool_fraction": ("pacing", "mempool_fraction"), "pace_poll_secs": ("pacing", "pace_poll_secs"),
    "retry_interval_secs": ("pacing", "retry_interval_secs"), "verify_after_secs": ("pacing", "verify_after_secs"),
    "btc_poll_secs": ("pacing", "btc_poll_secs"),
    "dead_cache_entries": ("limits", "dead_cache_entries"),
    "state": ("state", "path"),
    "status_file": ("report", "status_file"), "confirmed_file": ("report", "confirmed_file"),
    "confirmed_window": ("report", "confirmed_window"), "log_level": ("report", "log_level"),
}


def load_config(args):
    """DEFAULTS <- TOML file <- network preset (for unset URLs / fork height) <- CLI flags."""
    cfg = json.loads(json.dumps(DEFAULTS))  # deep copy
    if args.config:
        with open(args.config, "rb") as f:
            file_cfg = tomllib.load(f)
        for k, v in file_cfg.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    for flag, (section, key) in CLI_MAP.items():
        val = getattr(args, flag, None)
        if val is None:
            continue
        if section is None:
            cfg[key] = val
        else:
            cfg[section][key] = val
    preset = NETWORKS.get(cfg["network"])
    if preset is None:
        raise SystemExit(f"unknown network {cfg['network']!r}; choose from {', '.join(NETWORKS)}")
    if cfg["fork_height"] is None:
        cfg["fork_height"] = preset["fork_height"]
    if cfg["fork_height"] is None:
        raise SystemExit("--fork-height is required for network=regtest")
    cfg["btc"]["rpc_url"] = cfg["btc"]["rpc_url"] or preset["btc_url"]
    cfg["ecx"]["rpc_url"] = cfg["ecx"]["rpc_url"] or preset["ecx_url"]
    # Sending is opt-in on the command line only; a TOML file cannot switch it on by itself.
    cfg["run"]["dry_run"] = not args.live_send
    cfg["run"]["simulate"] = bool(args.simulate) or bool(cfg["run"].get("simulate"))
    if cfg["run"]["simulate"]:
        cfg["run"]["dry_run"] = True
    if cfg["run"]["mode"] not in ("backfill", "live"):
        raise SystemExit("mode must be 'backfill' or 'live'")
    return cfg


# --------------------------------------------------------------------------------------------
# JSON-RPC client
# --------------------------------------------------------------------------------------------

class RPCError(Exception):
    def __init__(self, method, code, message):
        super().__init__(f"{method}: {code} {message}")
        self.method, self.code, self.message = method, code, message


# Failures of the node or the transport (not verdicts on a transaction): the live daemon waits these out.
TRANSIENT_ERRORS = (RPCError, urllib.error.URLError, ConnectionError, TimeoutError, OSError, http.client.HTTPException)


def rpc_failure(err):
    """True for a JSON-RPC error that describes the node's state (warming up, IBD, internal/protocol error)
    rather than a verdict on the request; such errors are raised, never classified."""
    code = err.get("code") or 0
    return code in (-28, -20, -10) or code <= -32000


class RPC:
    """Single-connection JSON-RPC over urllib. Batches are capped at `batch_size` per HTTP request.
    Connection-level failures and -28 (node warming up) are retried with backoff; other JSON-RPC
    errors are returned/raised."""

    RETRIES = 8

    def __init__(self, name, url, cookie=None, user=None, password=None, timeout=300, batch_size=100):
        self.name = name
        self.url = url if url.endswith("/") else url + "/"
        if cookie:
            with open(cookie) as f:
                user, password = f.read().strip().split(":", 1)
        if user is None:
            # allow http://user:pass@host:port/ form
            from urllib.parse import urlsplit
            u = urlsplit(url)
            if u.username:
                user, password = u.username, u.password
                self.url = f"{u.scheme}://{u.hostname}:{u.port}{u.path or '/'}"
        self.auth = base64.b64encode(f"{user or ''}:{password or ''}".encode()).decode()
        self.timeout = timeout
        self.batch_size = batch_size
        self.calls = collections.Counter()   # method -> count
        self.seconds = collections.Counter() # method -> wall seconds
        self.total_calls = 0

    def _post(self, payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(self.url, data=data, headers={
            "Authorization": "Basic " + self.auth, "Content-Type": "application/json"})
        delay = 1.0
        for attempt in range(self.RETRIES):
            res, problem = None, None
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    res = json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:
                    res = json.loads(e.read())  # bitcoind returns JSON-RPC errors with HTTP 4xx/5xx
                except ValueError:
                    problem = e
            except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, http.client.HTTPException) as e:
                problem = e
            if res is not None:
                err = res.get("error") if isinstance(res, dict) else None
                if not (err and err.get("code") == -28):
                    return res
                problem = f"node not ready ({err.get('message')})"  # -28: warming up / loading; wait for it
            if attempt == self.RETRIES - 1:
                if isinstance(problem, BaseException):
                    raise problem
                return res
            log.warning("%s rpc: %s; retrying in %.0fs", self.name, problem, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)

    def try_call(self, method, *params):
        """Returns (result, error_dict_or_None)."""
        t0 = time.perf_counter()
        res = self._post({"jsonrpc": "1.0", "id": "garp", "method": method, "params": list(params)})
        self.calls[method] += 1
        self.seconds[method] += time.perf_counter() - t0
        self.total_calls += 1
        if res.get("error"):
            return None, res["error"]
        return res["result"], None

    def call(self, method, *params):
        result, err = self.try_call(method, *params)
        if err:
            raise RPCError(method, err.get("code"), err.get("message"))
        return result

    def batch(self, calls):
        """calls: [(method, params)] -> [(result, error)] in order; <= batch_size per request."""
        out = []
        for i in range(0, len(calls), self.batch_size):
            chunk = calls[i:i + self.batch_size]
            payload = [{"jsonrpc": "1.0", "id": k, "method": m, "params": p} for k, (m, p) in enumerate(chunk)]
            t0 = time.perf_counter()
            res = self._post(payload)
            key = "batch:" + chunk[0][0]
            self.calls[key] += 1
            self.seconds[key] += time.perf_counter() - t0
            self.total_calls += 1
            if not isinstance(res, list):  # a batch-level failure comes back as a single error object
                err = (res or {}).get("error") or {}
                raise RPCError(key, err.get("code"), err.get("message") or "non-list batch response")
            byid = {r["id"]: r for r in res}
            for k in range(len(chunk)):
                r = byid.get(k, {})
                out.append((r.get("result"), r.get("error")))
        return out

    def stats(self):
        return {m: {"calls": self.calls[m], "secs": round(self.seconds[m], 3)} for m in sorted(self.calls)}


# --------------------------------------------------------------------------------------------
# Reject-string classifier (exact strings of Core v31 / ecash-com/alphanet)
# --------------------------------------------------------------------------------------------

ACCEPTED, PRESENT, MISSING, RETRY, PACKAGE, CONFLICT, POLICY, DEAD = (
    "accepted", "present", "missing", "retry", "package", "conflict", "policy", "dead")

# First clause of the reject string (before ", ") -> (class, tag). Order does not matter: exact.
REJECT_TABLE = {
    # already there (someone else relayed it, or an earlier run / instance)
    "txn-already-in-mempool": (PRESENT, "mempool"),
    "txn-same-nonwitness-data-in-mempool": (PRESENT, "mempool"),
    "txn-already-known": (PRESENT, "confirmed"),
    "Transaction outputs already in utxo set": (PRESENT, "confirmed"),
    "Transaction already in block chain": (PRESENT, "confirmed"),
    # inputs missing: resolved by inspecting the inputs (dead / conflict / child-of-retry)
    "missing-inputs": (MISSING, "missing"),
    "bad-txns-inputs-missingorspent": (MISSING, "missing"),
    "Missing inputs": (MISSING, "missing"),
    # transient on a slower chain: offer again after the next ECX block
    "non-final": (RETRY, "nonfinal"),
    "non-BIP68-final": (RETRY, "bip68"),
    "TRUC-violation": (RETRY, "truc"),
    "too-large-cluster": (RETRY, "cluster"),
    "too-long-mempool-chain": (RETRY, "ancestors"),
    "too-many-ancestors": (RETRY, "ancestors"),
    "too-many-descendants": (RETRY, "ancestors"),
    "mempool full": (RETRY, "mempool-full"),
    "mempool min fee not met": (RETRY, "minfee"),
    "bad-txns-premature-spend-of-coinbase": (RETRY, "coinbase-maturity"),
    "too many potential replacements": (RETRY, "rbf-many"),
    "replacement-adds-unconfirmed": (RETRY, "rbf-unconfirmed"),
    "package-not-child-with-unconfirmed-parents": (RETRY, "package-shape"),
    # the parent alone is unacceptable; needs its child (CPFP / ephemeral dust) -> submitpackage
    "min relay fee not met": (PACKAGE, "minrelay"),
    "dust": (PACKAGE, "dust"),
    "missing-ephemeral-spends": (PACKAGE, "ephemeral"),
    "invalid-ephemeral-fee": (PACKAGE, "ephemeral"),
    # lost to a variant ECX already holds or mined: count, never fight
    "insufficient fee": (CONFLICT, "rbf-loss"),
    "replacement-failed": (CONFLICT, "rbf-loss"),   # v31 cluster mempool: does not improve the feerate diagram
    "txn-mempool-conflict": (CONFLICT, "mempool-conflict"),
    "bad-txns-spends-conflicting-tx": (CONFLICT, "mempool-conflict"),
}


def classify_reject(message):
    """Map a sendrawtransaction error message / testmempoolaccept reject-reason to (class, tag)."""
    if not message:
        return POLICY, "unknown"
    first = message.split(", ", 1)[0].strip()
    if first in REJECT_TABLE:
        return REJECT_TABLE[first]
    for key, val in REJECT_TABLE.items():
        if first.startswith(key):
            return val
    tag = first.split(" (")[0].replace(" ", "-")[:40] or "unknown"
    return POLICY, tag


# --------------------------------------------------------------------------------------------
# Persistent state (sqlite)
# --------------------------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS cursor (height INTEGER PRIMARY KEY, hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blocks (
    height INTEGER PRIMARY KEY, hash TEXT NOT NULL, ntx INTEGER, present INTEGER, injected INTEGER,
    dead INTEGER, dead_coinbase INTEGER, dead_ancestor INTEGER, retry INTEGER, package INTEGER,
    policy INTEGER, conflict INTEGER, conflict_split INTEGER, rpc INTEGER, ecx_rpc INTEGER, secs REAL,
    tags TEXT, processed_at INTEGER);
CREATE TABLE IF NOT EXISTS retry (
    txid TEXT PRIMARY KEY, height INTEGER NOT NULL, idx INTEGER NOT NULL, kind TEXT NOT NULL,
    tag TEXT, hex TEXT NOT NULL, parents TEXT NOT NULL, attempts INTEGER DEFAULT 0,
    first_seen INTEGER, last_attempt INTEGER, last_reason TEXT, fee REAL, vsize INTEGER);
CREATE INDEX IF NOT EXISTS retry_order ON retry(height, idx);
CREATE TABLE IF NOT EXISTS pending (
    txid TEXT PRIMARY KEY, height INTEGER NOT NULL, idx INTEGER NOT NULL, injected_at INTEGER NOT NULL,
    verified_at INTEGER);
CREATE INDEX IF NOT EXISTS pending_time ON pending(injected_at);
"""

CURSOR_KEEP = 200


class State:
    def __init__(self, path):
        self.path = path
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(retry)")}
        for col, typ in (("fee", "REAL"), ("vsize", "INTEGER")):  # 0.1.0 state files
            if col not in cols:
                self.db.execute(f"ALTER TABLE retry ADD COLUMN {col} {typ}")

    def begin(self):
        self.db.execute("BEGIN")

    def commit(self):
        self.db.execute("COMMIT")

    def rollback(self):
        self.db.execute("ROLLBACK")

    # meta
    def get(self, key, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else default

    def set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, str(value)))

    # cursor
    def cursor_top(self):
        r = self.db.execute("SELECT height, hash FROM cursor ORDER BY height DESC LIMIT 1").fetchone()
        return (r[0], r[1]) if r else None

    def cursor_rows_desc(self):
        return self.db.execute("SELECT height, hash FROM cursor ORDER BY height DESC").fetchall()

    def cursor_push(self, height, hash_):
        self.db.execute("INSERT OR REPLACE INTO cursor(height, hash) VALUES(?, ?)", (height, hash_))
        self.db.execute("DELETE FROM cursor WHERE height <= ?", (height - CURSOR_KEEP,))

    def rollback_to(self, height):
        """Forget everything above `height` (BTC reorg)."""
        for table in ("cursor", "blocks", "retry", "pending"):
            self.db.execute(f"DELETE FROM {table} WHERE height > ?", (height,))

    # retry queue
    def retry_add(self, txid, height, idx, kind, tag, hex_, vin, reason=None, fee=None, vsize=None):
        now = int(time.time())
        self.db.execute(
            "INSERT INTO retry(txid, height, idx, kind, tag, hex, parents, attempts, first_seen, last_attempt, last_reason,"
            " fee, vsize) VALUES(?,?,?,?,?,?,?,0,?,?,?,?,?) ON CONFLICT(txid) DO UPDATE SET kind=excluded.kind,"
            " tag=excluded.tag, last_attempt=excluded.last_attempt, last_reason=excluded.last_reason",
            (txid, height, idx, kind, tag, hex_, json.dumps([list(v) for v in vin]), now, now, reason, fee, vsize))

    def retry_touch(self, txid, kind, tag, reason):
        self.db.execute("UPDATE retry SET attempts=attempts+1, last_attempt=?, kind=?, tag=?, last_reason=? WHERE txid=?",
                        (int(time.time()), kind, tag, reason, txid))

    def retry_remove(self, txid):
        self.db.execute("DELETE FROM retry WHERE txid=?", (txid,))

    def retry_rows(self):
        cur = self.db.execute("SELECT txid, height, idx, kind, tag, hex, parents, attempts, fee, vsize FROM retry"
                              " ORDER BY height, idx")
        keys = ("txid", "height", "idx", "kind", "tag", "hex", "parents", "attempts", "fee", "vsize")
        return [dict(zip(keys, r)) for r in cur]

    def retry_get(self, txid):
        r = self.db.execute("SELECT height, idx, parents FROM retry WHERE txid=?", (txid,)).fetchone()
        return {"height": r[0], "idx": r[1], "parents": json.loads(r[2])} if r else None

    def retry_kinds(self):
        return {r[0]: r[1] for r in self.db.execute("SELECT txid, kind FROM retry")}

    def retry_count(self):
        return self.db.execute("SELECT COUNT(*) FROM retry").fetchone()[0]

    def retry_tags(self):
        return dict(self.db.execute("SELECT tag, COUNT(*) FROM retry GROUP BY tag").fetchall())

    # pending (injected, unconfirmed on ECX)
    def pending_add(self, txid, height, idx):
        self.db.execute("INSERT OR REPLACE INTO pending(txid, height, idx, injected_at) VALUES(?,?,?,?)",
                        (txid, height, idx, int(time.time())))

    def pending_get(self, txid):
        r = self.db.execute("SELECT txid, height, idx FROM pending WHERE txid=?", (txid,)).fetchone()
        return tuple(r) if r else None

    def pending_remove_many(self, txids):
        return self.db.executemany("DELETE FROM pending WHERE txid=?", [(t,) for t in txids]).rowcount

    def pending_older_than(self, ts, limit):
        cur = self.db.execute("SELECT txid, height, idx FROM pending WHERE injected_at < ? AND"
                              " (verified_at IS NULL OR verified_at < ?) ORDER BY injected_at LIMIT ?", (ts, ts, limit))
        return cur.fetchall()

    def pending_mark_verified(self, txids):
        now = int(time.time())
        for txid in txids:
            self.db.execute("UPDATE pending SET verified_at=? WHERE txid=?", (now, txid))

    def pending_count(self):
        return self.db.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    # blocks
    def block_add(self, row):
        cols = ",".join(row)
        self.db.execute(f"INSERT OR REPLACE INTO blocks({cols}) VALUES({','.join('?' * len(row))})", tuple(row.values()))

    def block_rows(self, limit):
        cur = self.db.execute("SELECT * FROM blocks ORDER BY height DESC LIMIT ?", (limit,))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur][::-1]

    def totals(self):
        cur = self.db.execute("SELECT COUNT(*), COALESCE(SUM(ntx),0), COALESCE(SUM(present),0), COALESCE(SUM(injected),0),"
                              " COALESCE(SUM(dead),0), COALESCE(SUM(dead_coinbase),0), COALESCE(SUM(dead_ancestor),0),"
                              " COALESCE(SUM(retry),0), COALESCE(SUM(package),0), COALESCE(SUM(policy),0),"
                              " COALESCE(SUM(conflict),0), COALESCE(SUM(conflict_split),0), COALESCE(SUM(rpc),0) FROM blocks")
        r = cur.fetchone()
        keys = ("blocks", "tx", "present", "injected", "dead", "dead_coinbase", "dead_ancestor", "retry",
                "package", "policy", "conflict", "conflict_split", "rpc")
        return dict(zip(keys, r))

    def rolling_coverage(self, n):
        r = self.db.execute("SELECT COALESCE(SUM(ntx),0), COALESCE(SUM(present),0), COALESCE(SUM(injected),0) FROM"
                            " (SELECT ntx, present, injected FROM blocks ORDER BY height DESC LIMIT ?)", (n,)).fetchone()
        return (r[1] + r[2]) / r[0] if r[0] else None


# --------------------------------------------------------------------------------------------
# Dead-cache: a bounded, generational set of txids known to be unspendable on ECX.
# It is a cache, not ground truth: a miss costs RPC (presence + submit) and gives the same answer.
# --------------------------------------------------------------------------------------------

class DeadCache:
    def __init__(self, cap):
        self.cap = max(1000, cap)
        self.cur, self.old = set(), set()

    def add(self, txid):
        if len(self.cur) >= self.cap // 2:
            self.old, self.cur = self.cur, set()
        self.cur.add(txid)

    def __contains__(self, txid):
        return txid in self.cur or txid in self.old

    def __len__(self):
        return len(self.cur) + len(self.old)

    def clear(self):
        self.cur, self.old = set(), set()


# --------------------------------------------------------------------------------------------
# The bridge
# --------------------------------------------------------------------------------------------

class BlockTx:
    __slots__ = ("txid", "hex", "vin", "fee", "vsize", "idx")

    def __init__(self, idx, t):
        self.idx = idx
        self.txid = t["txid"]
        self.hex = t["hex"]
        self.vin = [(v["txid"], v["vout"]) for v in t["vin"]]
        self.fee = t.get("fee")
        self.vsize = t.get("vsize")

    @property
    def feerate(self):
        """BTC/kvB, or None when the BTC node did not report the fee."""
        if self.fee is None or not self.vsize:
            return None
        return float(self.fee) * 1000.0 / self.vsize


class BlockStats:
    def __init__(self, height, hash_, ntx):
        self.height, self.hash, self.ntx = height, hash_, ntx
        self.counts = collections.Counter()
        self.tags = collections.Counter()
        self.rpc0 = 0
        self.t0 = time.time()

    def add(self, cls, tag):
        self.counts[cls] += 1
        if cls in (RETRY, PACKAGE, POLICY, CONFLICT, DEAD):
            self.tags[f"{cls}:{tag}"] += 1

    def row(self, rpc, ecx_rpc):
        c = self.counts
        return {
            "height": self.height, "hash": self.hash, "ntx": self.ntx, "present": c[PRESENT], "injected": c[ACCEPTED],
            "dead": c[DEAD], "dead_coinbase": self.tags["dead:coinbase"], "dead_ancestor": self.tags["dead:ancestor"],
            "retry": c[RETRY], "package": c[PACKAGE], "policy": c[POLICY], "conflict": c[CONFLICT],
            "conflict_split": self.tags["conflict:split"], "rpc": rpc, "ecx_rpc": ecx_rpc,
            "secs": round(time.time() - self.t0, 3),
            "tags": json.dumps(dict(self.tags), sort_keys=True), "processed_at": int(time.time()),
        }


class Bridge:
    MAXBURN = 21_000_000  # sendrawtransaction maxburnamount: never refuse a BTC-confirmed tx for burning

    def __init__(self, cfg):
        self.cfg = cfg
        self.fork_height = int(cfg["fork_height"])
        self.dry_run = cfg["run"]["dry_run"]
        self.simulate = cfg["run"]["simulate"]
        self.mode = cfg["run"]["mode"]
        batch = max(1, min(100, int(cfg["limits"]["presence_batch"])))
        self.btc = RPC("btc", cfg["btc"]["rpc_url"], cfg["btc"]["cookie"], cfg["btc"]["user"], cfg["btc"]["password"],
                       batch_size=batch)
        self.ecx = RPC("ecx", cfg["ecx"]["rpc_url"], cfg["ecx"]["cookie"], cfg["ecx"]["user"], cfg["ecx"]["password"],
                       batch_size=batch)
        self.state = State(cfg["state"]["path"])
        self.dead = DeadCache(cfg["limits"]["dead_cache_entries"])
        self.coinbases = DeadCache(max(10_000, cfg["limits"]["dead_cache_entries"] // 100))
        self.retry_kinds = self.state.retry_kinds()      # txid -> 'retry' | 'package' (in-memory mirror)
        self.virtual = set()                             # simulate: would-be-injected txids
        self.virtual_spent = set()                       # simulate: (txid, vout) spent by a virtual tx
        self.window = collections.OrderedDict()          # height -> (hash, [txids]) for --confirmed-file
        self.started = time.time()
        self.paused = False
        self.last_sweep = 0.0
        self.ecx_tip = None
        self.ecx_tip_height = None
        self.mempool_info = {}
        self.submits = 0
        self.pace_at = 0                                 # self.submits at the last pacing check
        self._blk_cache = None                           # (height, block): last BTC block re-read for a re-queue
        self.stop = False
        self.crash_after = int(os.environ.get("GARP_CRASH_AFTER_SUBMITS", "0") or 0)  # test hook

    # ---- startup ---------------------------------------------------------------------------
    def startup_checks(self):
        log.info("garp_replay %s  network=%s fork=%d  mode=%s  %s", __version__, self.cfg["network"], self.fork_height,
                 self.mode, "SIMULATE (dry run + virtual overlay)" if self.simulate else
                 "DRY RUN (testmempoolaccept; pass --live-send to submit)" if self.dry_run else "LIVE (sendrawtransaction)")
        b, e = self.wait_for_nodes()
        shared = self.fork_height - 1
        if shared >= 0:
            hb, he = self.btc.call("getblockhash", shared), self.ecx.call("getblockhash", shared)
            if hb != he:
                raise SystemExit(f"btc and ecx disagree on block {shared} (the last shared block): wrong fork height or nodes")
            if b["blocks"] >= self.fork_height and e["blocks"] >= self.fork_height and \
                    self.btc.call("getblockhash", self.fork_height) == self.ecx.call("getblockhash", self.fork_height):
                log.warning("btc and ecx still agree on block %d: the fork has not happened yet", self.fork_height)
        prev_mode = self.state.get("dry_run")
        if prev_mode is not None and (prev_mode == "1") != self.dry_run:
            log.warning("state file was last used in %s mode", "dry-run" if prev_mode == "1" else "live")
        self.state.set("dry_run", "1" if self.dry_run else "0")
        if self.state.get("ecx_tip") is None:  # persisted from the first run on, so ECX blocks mined between
            self.state.set("ecx_tip", e["bestblockhash"])   # backfill runs are walked too
            self.state.set("ecx_tip_height", e["blocks"])
        self.ecx_tip = self.state.get("ecx_tip")
        h = self.state.get("ecx_tip_height")
        self.ecx_tip_height = int(h) if h is not None else None
        if self.cfg["report"]["confirmed_file"]:
            self.restore_window()

    def wait_for_nodes(self):
        """Both nodes must be usable: same chain, out of warm-up, ECX out of initial block download with a
        synced txindex and a chain that reaches the last shared block (walking blocks against an ECX node
        that is syncing, reindexing or rolled back would record everything as conflict/dead). Live mode
        waits for this; backfill refuses. Returns both getblockchaininfo results."""
        shared = self.fork_height - 1
        wait = max(1.0, float(self.cfg["pacing"]["btc_poll_secs"]))
        while not self.stop:
            b, e = self.btc.call("getblockchaininfo"), self.ecx.call("getblockchaininfo")
            if b["chain"] != e["chain"]:
                raise SystemExit(f"chain mismatch: btc={b['chain']} ecx={e['chain']}")
            problems = []
            if e["initialblockdownload"]:
                problems.append("ecx node is in initial block download")
            if e["blocks"] < shared:
                problems.append(f"ecx tip {e['blocks']} is below the last shared block {shared} "
                                "(syncing, reindexing or rolled back?)")
            if b["blocks"] < shared:
                problems.append(f"btc tip {b['blocks']} is below the last shared block {shared}")
            if self.cfg["ecx"]["require_txindex"]:
                info, err = self.ecx.try_call("getindexinfo", "txindex")
                idx = (info or {}).get("txindex") if not err else None
                if not idx:
                    raise SystemExit("ECX node needs txindex=1; presence checks depend on it "
                                     "(set ecx.require_txindex=false to override at your own risk)")
                if not idx.get("synced"):
                    problems.append(f"ecx txindex is not synced (at {idx.get('best_block_height')} of {e['blocks']})")
                elif idx.get("best_block_height", 0) > e["blocks"]:
                    problems.append("ecx txindex is ahead of the chainstate (chainstate rolled back?)")
            if not problems:
                log.info("btc %s tip %d (%s)  ecx %s tip %d (%s)", b["chain"], b["blocks"], b["bestblockhash"][:16],
                         e["chain"], e["blocks"], e["bestblockhash"][:16])
                if b["initialblockdownload"]:
                    log.warning("btc node is in initial block download; results will lag")
                return b, e
            if self.mode == "backfill":
                raise SystemExit("node not usable: " + "; ".join(problems))
            log.warning("waiting for nodes: %s", "; ".join(problems))
            time.sleep(wait)
        raise SystemExit("stopped while waiting for nodes")

    def next_height(self):
        top = self.state.cursor_top()
        if top:
            return top[0] + 1
        start = self.cfg["run"]["start_height"]
        start = self.fork_height if start is None else int(start)
        if start < self.fork_height:
            log.info("start height %d is below the fork height; clamping to %d (pre-fork blocks are shared)",
                     start, self.fork_height)
            start = self.fork_height
        return start

    # ---- main loop -------------------------------------------------------------------------
    def run(self):
        started, revalidate, delay = False, False, max(1.0, float(self.cfg["pacing"]["btc_poll_secs"]))
        while not self.stop:
            try:
                if not started:
                    self.startup_checks()
                    self.follow_ecx()
                    self.write_status()
                    started = True
                if revalidate:  # after an outage: the ECX node may be back in IBD, reindexing or rolled back
                    self.wait_for_nodes()
                    revalidate = False
                processed = self.catch_up()
                if self.mode == "backfill":
                    self.sweep()
                    self.write_status()
                    top = self.state.cursor_top()
                    log.info("backfill complete at cursor %s; retry=%d pending=%d", top[0] if top else None,
                             self.state.retry_count(), self.state.pending_count())
                    return
                if not self.follow_ecx() and time.time() - self.last_sweep >= self.cfg["pacing"]["retry_interval_secs"]:
                    self.sweep()
                self.write_status()
                if not processed and not self.stop:
                    # long-poll the BTC node; returns early on a new block (works on Core 29 and 31)
                    self.btc.try_call("waitfornewblock", int(self.cfg["pacing"]["btc_poll_secs"] * 1000))
                delay = max(1.0, float(self.cfg["pacing"]["btc_poll_secs"]))
            except TRANSIENT_ERRORS as e:
                if self.mode == "backfill":
                    raise
                self.recover(e, delay)
                delay = min(delay * 2, 300.0)
                revalidate = True
        log.info("stopped; state is durable, restart to resume")

    def recover(self, exc, delay):
        """Live mode: a node restart, warm-up or outage must not kill the daemon. Every state write is
        transactional, so whatever was interrupted is simply re-walked once the nodes are back."""
        log.error("rpc failure: %s; retrying in %.0fs", exc, delay)
        if self.state.db.in_transaction:
            self.state.rollback()
        self.retry_kinds = self.state.retry_kinds()  # the in-memory mirror may be ahead of the rolled-back state
        self._blk_cache = None
        t0 = time.time()
        while not self.stop and time.time() - t0 < delay:
            time.sleep(1)

    def catch_up(self):
        """Process BTC blocks from the cursor to the tip (minus confirmations_required-1)."""
        n = 0
        while not self.stop:
            if not self.check_reorg():
                return n
            tip = self.btc.call("getblockcount")
            target = tip - (int(self.cfg["run"]["confirmations_required"]) - 1)
            h = self.next_height()
            if h > target:
                return n
            self.process_block(h)
            n += 1

    # ---- reorg -----------------------------------------------------------------------------
    def check_reorg(self):
        """True when the cursor is consistent with the BTC chain (possibly after rolling back to the fork
        point); False when the BTC tip is below the cursor and nothing can be decided yet (invalidated tip,
        node restoring): then wait, never roll back on the strength of a missing block."""
        top = self.state.cursor_top()
        if top is None:
            return True
        actual, err = self.btc.try_call("getblockhash", top[0])
        if err:
            if err.get("code") == -8:  # block height out of range
                log.warning("btc tip is below the cursor %d; waiting for the chain to reach it again", top[0])
                return False
            raise RPCError("getblockhash", err.get("code"), err.get("message"))
        if actual == top[1]:
            return True
        fork = None
        for h, hash_ in self.state.cursor_rows_desc():
            actual, err = self.btc.try_call("getblockhash", h)
            if not err and actual == hash_:
                fork = h
                break
        if fork is None:
            fork = min(h for h, _ in self.state.cursor_rows_desc()) - 1
        log.warning("BTC reorg: cursor %d (%s) is no longer in the main chain; rolling back to %d",
                    top[0], top[1][:16], fork)
        self.state.begin()
        self.state.rollback_to(fork)
        self.state.commit()
        self.retry_kinds = self.state.retry_kinds()
        self.dead.clear()
        self.coinbases.clear()
        self.virtual.clear()
        self.virtual_spent.clear()
        for h in [h for h in self.window if h > fork]:
            del self.window[h]
        return True

    # ---- one BTC block ---------------------------------------------------------------------
    def fetch_block(self, height):
        bh = self.btc.call("getblockhash", height)
        blk = self.btc.call("getblock", bh, 2)
        return bh, blk

    def process_block(self, height):
        bh, blk = self.fetch_block(height)
        top = self.state.cursor_top()
        if top and blk.get("previousblockhash") != top[1]:
            log.warning("block %d does not extend the cursor; re-checking for a reorg", height)
            time.sleep(1)
            self.check_reorg()
            return
        txs = [BlockTx(i, t) for i, t in enumerate(blk["tx"]) if i > 0]
        if height >= self.fork_height:
            self.coinbases.add(blk["tx"][0]["txid"])  # post-fork BTC coinbase: dead on ECX by construction
        stats = BlockStats(height, bh, len(txs))
        self.pace()
        rpc0, ecx0 = self.btc.total_calls + self.ecx.total_calls, self.ecx.total_calls
        self.state.begin()
        try:
            ctx = _BlockContext(self, txs)
            for tx in txs:
                cls, tag = self.handle_tx(tx, height, ctx)
                ctx.done.add(tx.txid)
                stats.add(cls, tag)
                if self.submits - self.pace_at >= PACE_EVERY:
                    self.pace()
            self.state.cursor_push(height, bh)
            stats_row = stats.row(self.btc.total_calls + self.ecx.total_calls - rpc0, self.ecx.total_calls - ecx0)
            self.state.block_add(stats_row)
            self.state.commit()
        except BaseException:
            self.state.rollback()
            raise
        if self.cfg["report"]["confirmed_file"]:
            self.window[height] = (bh, [t.txid for t in txs])
            self.write_confirmed_file()
        self.log_block(stats_row)
        self.write_status()

    def handle_tx(self, tx, height, ctx):
        """The per-transaction pipeline. Returns (class, tag)."""
        # 1. dead-cache: a known-dead ancestor makes this tx dead, no RPC
        for ptx, _ in tx.vin:
            if ptx in self.coinbases:
                self.dead.add(tx.txid)
                return DEAD, "coinbase"
            if ptx in self.dead:
                self.dead.add(tx.txid)
                return DEAD, "ancestor"
        # 2. retry-set: a parent still waiting means this one waits too (or completes a package)
        waiting = [ptx for ptx, _ in tx.vin if ptx in self.retry_kinds]
        if waiting:
            return self.child_of_waiting(tx, height, waiting)
        # 3. presence via txindex (batched, lookahead)
        pres = ctx.presence(tx)
        if pres:
            return self.present(tx, height, pres)
        # 3b. below ECX's dynamic mempool floor: do not even try, keep for later (0 RPC)
        minfee = self.mempool_info.get("mempoolminfee")
        if minfee and tx.feerate is not None and tx.feerate < float(minfee) and \
                float(minfee) > float(self.mempool_info.get("minrelaytxfee", 0)):
            self.queue(tx, height, RETRY, "minfee", "below mempoolminfee (not submitted)")
            return RETRY, "minfee"
        # 4. submit
        cls, tag, reason = self.submit_one(tx)
        return self.apply_result(tx, height, cls, tag, reason)

    def apply_result(self, tx, height, cls, tag, reason):
        if cls == ACCEPTED:
            self.accepted(tx, height)
            return ACCEPTED, tag
        if cls == PRESENT:
            return self.present(tx, height, tag)
        if cls == MISSING:
            cls, tag = self.resolve_missing(tx)
            if cls == ACCEPTED:  # simulate overlay: all inputs virtually present
                self.accepted(tx, height)
                return ACCEPTED, "virtual"
            if cls == RETRY:
                if tag == "dry-run-parent":
                    self.dequeue(tx.txid)  # cannot be evaluated without injecting the parent; reported only
                else:
                    self.queue(tx, height, RETRY, tag, reason)
            else:
                self.dequeue(tx.txid)
                self.dead.add(tx.txid)
            return cls, tag
        if cls in (RETRY, PACKAGE):
            self.queue(tx, height, cls, tag, reason)
            return cls, tag
        if cls == CONFLICT:
            self.dequeue(tx.txid)
            self.dead.add(tx.txid)
            log.info("conflict %s at %d: %s", tx.txid, height, reason)
            return CONFLICT, tag
        # POLICY: recorded, retried rarely (max_attempts), then dropped
        self.queue(tx, height, POLICY, tag, reason)
        log.info("policy reject %s at %d: %s", tx.txid, height, reason)
        return POLICY, tag

    def present(self, tx, height, tag):
        """ECX already has it. An unconfirmed one is tracked in `pending` like our own injections, so
        the eviction re-queue covers transactions other bridges / gossip delivered as well."""
        self.dequeue(tx.txid)
        if tag == "mempool":
            self.state.pending_add(tx.txid, height, tx.idx)
        return PRESENT, tag

    def child_of_waiting(self, tx, height, waiting):
        """tx spends a parent that is in the retry queue. If every waiting parent is a
        package-kind parent (unacceptable alone), try a child-with-parents package now;
        otherwise queue the child behind its parent (0 RPC)."""
        kinds = {self.retry_kinds[p] for p in waiting}
        reason = None
        if kinds == {PACKAGE}:
            cls, tag, reason = self.submit_package(waiting, tx)
            if cls == ACCEPTED:
                self.package_accepted(waiting, tx, height)
                return ACCEPTED, "package"
            if cls == PRESENT:  # the parents got in by another route; the child stands on its own now
                for p in waiting:
                    self.dequeue(p)
                cls, tag, reason = self.submit_one(tx)
                return self.apply_result(tx, height, cls, tag, reason)
        self.queue(tx, height, RETRY, "child-of-retry", reason)
        return RETRY, "child-of-retry"

    def resolve_missing(self, tx):
        """ECX said missing-inputs. Look at each input on ECX:
             coin exists (chain or mempool)                    -> this input is fine
             parent in our retry queue                         -> RETRY(child-of-retry)
             parent present on ECX but the coin is gone        -> spent differently on ECX: CONFLICT(split)
             parent absent, known dead                         -> DEAD(ancestor)
             parent absent but recorded in `pending` (we injected it, or saw it in the ECX mempool)
                                                               -> ECX dropped it (eviction, expiry, restart):
                                                                  re-queue the parent from its BTC block now,
                                                                  RETRY(child-of-retry)
             parent absent with no record at all               -> DEAD(ancestor): blocks are walked in order, so
                                                                  every post-fork parent was offered before its
                                                                  child; absent + never accepted = dead/conflict
           Absence alone is never cached as dead: the `pending` record is what tells eviction from death.
           In a dry run a parent remembered in `pending` is a would-be injection of an earlier run
           (reported as dry-run-parent); in --simulate it counts as present."""
        split = dead = waiting = dry_parent = False
        for ptx, n in tx.vin:
            if ptx in self.retry_kinds:
                waiting = True
                continue
            if ptx in self.virtual:  # would have been injected by this dry run
                if self.simulate:
                    if (ptx, n) in self.virtual_spent:
                        split = True
                else:
                    dry_parent = True
                continue
            coin, _ = self.ecx.try_call("gettxout", ptx, n, True)
            if coin is not None:
                continue
            if ptx in self.coinbases or ptx in self.dead:
                dead = True
                continue
            parent, _ = self.ecx.try_call("getrawtransaction", ptx, 1)
            if parent is not None:
                split = True
                continue
            row = self.state.pending_get(ptx)
            if row is not None:
                if self.dry_run:
                    if not self.simulate:
                        dry_parent = True
                    continue
                log.info("parent %s of %s is no longer on ECX; re-queueing it", ptx[:16], tx.txid[:16])
                self.requeue_from_btc([row])
                waiting = True
                continue
            dead = True
            self.dead.add(ptx)
        if split:
            return CONFLICT, "split"
        if dead:
            return DEAD, "ancestor"
        if waiting:
            return RETRY, "child-of-retry"
        if dry_parent:
            return RETRY, "dry-run-parent"
        if self.simulate:
            return ACCEPTED, "virtual"
        # every input looks spendable yet ECX said missing: parent evicted between calls, or a race
        return RETRY, "orphan"

    def accepted(self, tx, height):
        self.dequeue(tx.txid)
        if self.dry_run:
            self.virtual.add(tx.txid)
            if self.simulate:
                for ptx, n in tx.vin:
                    self.virtual_spent.add((ptx, n))
        # live: injected, awaiting ECX confirmation (verifier + eviction re-queue);
        # dry run: remembered as a would-be injection so a restart does not turn its children into dead:ancestor
        self.state.pending_add(tx.txid, height, tx.idx)

    def package_accepted(self, parents, child, height):
        for p in parents:
            row = self.state.retry_get(p)
            self.dequeue(p)
            if self.dry_run:
                self.virtual.add(p)
                if self.simulate and row:
                    for ptx, n in row["parents"]:
                        self.virtual_spent.add((ptx, n))
            self.state.pending_add(p, row["height"] if row else height, -1)
        self.accepted(child, height)

    def requeue_from_btc(self, rows):
        """Transactions ECX no longer holds (evicted, expired, dropped on restart): back into the retry
        queue with their hex re-read from the BTC block. Runs inside the caller's transaction."""
        by_height = collections.defaultdict(set)
        for txid, height, _ in rows:
            by_height[height].add(txid)
        n = 0
        for height, want in sorted(by_height.items()):
            if self._blk_cache and self._blk_cache[0] == height:
                blk = self._blk_cache[1]
            else:
                _, blk = self.fetch_block(height)
                self._blk_cache = (height, blk)
            for i, t in enumerate(blk["tx"]):
                if t["txid"] in want:
                    self.queue(BlockTx(i, t), height, RETRY, "evicted", "no longer in the ECX mempool")
                    n += 1
        self.state.pending_remove_many([r[0] for r in rows])
        return n

    def queue(self, tx, height, cls, tag, reason):
        kind = cls if cls in (PACKAGE, POLICY) else RETRY
        if tx.txid in self.retry_kinds:
            self.state.retry_touch(tx.txid, kind, tag, reason)
        else:
            self.state.retry_add(tx.txid, height, tx.idx, kind, tag, tx.hex, tx.vin, reason, tx.fee, tx.vsize)
        self.retry_kinds[tx.txid] = kind

    def dequeue(self, txid):
        if txid in self.retry_kinds:
            self.state.retry_remove(txid)
            del self.retry_kinds[txid]

    # ---- ECX submission --------------------------------------------------------------------
    def submit_one(self, tx):
        """-> (class, tag, reason). Dry run uses testmempoolaccept, one tx per call."""
        self.submits += 1
        delay_ms = float(self.cfg["pacing"].get("submit_delay_ms") or 0)
        if delay_ms and not self.dry_run:  # optional jitter to de-synchronise a fleet of instances
            time.sleep(random.uniform(0, delay_ms) / 1000.0)
        if self.crash_after and self.submits >= self.crash_after:
            log.error("GARP_CRASH_AFTER_SUBMITS reached: simulating a crash")
            os._exit(137)
        if self.dry_run:
            res, err = self.ecx.try_call("testmempoolaccept", [tx.hex], 0)
            if err:
                if rpc_failure(err):
                    raise RPCError("testmempoolaccept", err.get("code"), err.get("message"))
                return POLICY, "rpc-error", err.get("message")
            r = res[0]
            if r.get("allowed"):
                return ACCEPTED, "injected", None
            reason = r.get("reject-details") or r.get("reject-reason") or ""
            cls, tag = classify_reject(r.get("reject-reason") or reason)
            return cls, tag, reason
        _, err = self.ecx.try_call("sendrawtransaction", tx.hex, 0, self.MAXBURN)
        if err is None:
            return ACCEPTED, "injected", None
        if rpc_failure(err):
            raise RPCError("sendrawtransaction", err.get("code"), err.get("message"))
        cls, tag = classify_reject(err.get("message", ""))
        return cls, tag, err.get("message")

    def submit_package(self, parent_txids, child):
        """child-with-parents package: parents come from the retry queue (their hex is stored)."""
        rows = {r["txid"]: r for r in self.state.retry_rows() if r["txid"] in parent_txids}
        if len(rows) != len(parent_txids):
            return MISSING, "missing", "package parent not in queue"
        hexes = [rows[p]["hex"] for p in sorted(parent_txids, key=lambda p: (rows[p]["height"], rows[p]["idx"]))]
        hexes.append(child.hex)
        self.submits += 1
        if self.dry_run:
            res, err = self.ecx.try_call("testmempoolaccept", hexes, 0)
            if err:
                if rpc_failure(err):
                    raise RPCError("testmempoolaccept", err.get("code"), err.get("message"))
                return POLICY, "rpc-error", err.get("message")
            bad = [(i, r) for i, r in enumerate(res) if not r.get("allowed")]
            if not bad:
                return ACCEPTED, "package", None
            i, r = bad[0]
            reason = r.get("reject-details") or r.get("reject-reason") or "package rejected"
            cls, tag = classify_reject(r.get("reject-reason") or reason)
            if cls == PACKAGE and i < len(hexes) - 1:
                # testmempoolaccept judges every member on its own fee (Core's test path runs with
                # package_feerates=false), so a CPFP / zero-fee-parent package can never pass here although
                # submitpackage accepts it. Estimate instead: does the package feerate, from the BTC node's fee
                # data, clear ECX's minimum relay fee? (The child's scripts are not evaluated: same caveat as
                # any descendant of a would-be injection in a dry run.)
                fees = [rows[p]["fee"] for p in parent_txids] + [child.fee]
                sizes = [rows[p]["vsize"] for p in parent_txids] + [child.vsize]
                if all(f is not None for f in fees) and all(sizes):
                    minrelay = float(self.mempool_info.get("minrelaytxfee") or 0.00001)
                    if float(sum(fees)) * 1000.0 / sum(sizes) >= minrelay:
                        self.state.set("package_estimated", int(self.state.get("package_estimated", 0)) + 1)
                        return ACCEPTED, "package-estimated", None
                else:
                    log.warning("package %s: BTC node reported no fee data; cannot estimate in a dry run", child.txid[:16])
            return cls, tag, reason
        res, err = self.ecx.try_call("submitpackage", hexes, 0, self.MAXBURN)
        if err:
            if rpc_failure(err):
                raise RPCError("submitpackage", err.get("code"), err.get("message"))
            cls, tag = classify_reject(err.get("message", ""))
            return cls, tag, err.get("message")
        if res.get("package_msg") == "success":
            return ACCEPTED, "package", None
        for r in res.get("tx-results", {}).values():
            if r.get("error"):
                cls, tag = classify_reject(r["error"])
                return cls, tag, r["error"]
        return POLICY, "package", res.get("package_msg")

    # ---- retry sweep ---------------------------------------------------------------------------
    def sweep(self):
        """Re-offer queued transactions in (btc height, block index) order."""
        self.last_sweep = time.time()
        rows = self.state.retry_rows()
        if not rows:
            return
        self.pace()
        counts = collections.Counter()
        self.state.begin()
        try:
            for row in rows:
                if self.stop:
                    break
                tx = _RetryTx(row)
                # Only policy rejects are ever given up (and only dropped, never marked dead: if ECX still
                # lacks the parent when a child comes along, the child's own missing-inputs resolution says so).
                # Transient kinds (cluster/minfee/mempool-full/locks/child-of-retry/evicted) wait for ECX
                # blockspace for as long as it takes.
                if row["kind"] == POLICY and row["attempts"] >= int(self.cfg["run"]["max_attempts"]):
                    log.warning("dropping policy-rejected %s after %d attempts (%s)", tx.txid, row["attempts"], row["tag"])
                    self.dequeue(tx.txid)
                    counts["gave-up"] += 1
                    continue
                waiting = [p for p in tx.parents if p in self.retry_kinds]
                if waiting:
                    kinds = {self.retry_kinds[p] for p in waiting}
                    if kinds != {PACKAGE}:
                        counts["waiting"] += 1
                        continue
                    cls, tag, reason = self.submit_package(waiting, tx)
                    if cls == ACCEPTED:
                        self.package_accepted(waiting, tx, row["height"])
                        counts["injected"] += 1
                        continue
                    self.state.retry_touch(tx.txid, RETRY, "child-of-retry", reason)
                    counts["waiting"] += 1
                    continue
                pres, err = self.ecx.try_call("getrawtransaction", tx.txid, 1)
                if err and err.get("code") != -5:
                    raise RPCError("getrawtransaction", err.get("code"), err.get("message"))
                if pres is not None:
                    self.present(tx, row["height"], "confirmed" if pres.get("blockhash") else "mempool")
                    counts["present"] += 1
                    continue
                cls, tag, reason = self.submit_one(tx)
                cls, tag = self.apply_result(tx, row["height"], cls, tag, reason)
                counts[cls] += 1
                if self.submits - self.pace_at >= PACE_EVERY:
                    self.pace()
            self.state.commit()
        except BaseException:
            self.state.rollback()
            raise
        for k, v in counts.items():
            self.state.set(f"sweep_{k}", int(self.state.get(f"sweep_{k}", 0)) + v)
        log.info("sweep: %s (queue now %d)", " ".join(f"{k}={v}" for k, v in sorted(counts.items())),
                 self.state.retry_count())

    # ---- ECX follower: pending verifier + block-triggered sweep --------------------------------
    def follow_ecx(self):
        tip = self.ecx.call("getbestblockhash")
        if tip == self.ecx_tip:
            return False
        # Walk the new ECX blocks back to the last one we saw, marking pending rows confirmed. The walk is
        # bounded by height: (new tip height - last seen height) blocks, plus ECX_REORG_DEPTH when the last
        # seen tip is no longer in the chain (an ECX reorg) - never the whole chain. A transaction confirmed
        # in a reorged-out ECX block is not re-tracked (it is back in the ECX mempool, or ECX's own relay
        # fetches it again); the pending verifier covers whatever falls outside the bound.
        hashes, h, tip_height = [], tip, None
        limit = ECX_REORG_DEPTH if self.ecx_tip_height is None else ECX_WALK_MAX
        while h and h != self.ecx_tip and len(hashes) < limit:
            hdr = self.ecx.call("getblockheader", h)
            if tip_height is None:
                tip_height = hdr["height"]
                if self.ecx_tip_height is not None:
                    limit = min(ECX_WALK_MAX, max(0, tip_height - self.ecx_tip_height) + ECX_REORG_DEPTH)
            hashes.append(h)
            h = hdr.get("previousblockhash")
        if h != self.ecx_tip:
            log.warning("ecx reorg: the last seen tip %s is no longer in the chain; walked %d blocks back from %d",
                        self.ecx_tip[:16], len(hashes), tip_height)
            if self.ecx_tip_height is not None and tip_height < self.ecx_tip_height - ECX_REORG_DEPTH:
                log.error("ecx tip went back from %d to %d; re-validating the node", self.ecx_tip_height, tip_height)
                self.wait_for_nodes()
        confirmed = 0
        if self.state.pending_count():  # only read full blocks when there is something to retire
            self.state.begin()
            try:
                for h in reversed(hashes):
                    blk = self.ecx.call("getblock", h, 1)
                    confirmed += self.state.pending_remove_many(blk["tx"])
                self.state.commit()
            except BaseException:
                self.state.rollback()
                raise
        self.ecx_tip, self.ecx_tip_height = tip, tip_height
        self.state.set("ecx_tip", tip)
        self.state.set("ecx_tip_height", tip_height)
        log.info("ecx block(s): %d new, %d pending confirmed", len(hashes), confirmed)
        self.verify_pending()
        self.sweep()
        return True

    def verify_pending(self):
        """Injected long ago but still unconfirmed: still in the mempool? If not, re-queue."""
        if self.dry_run:
            return  # nothing was injected; pending only remembers would-be injections
        cutoff = int(time.time()) - int(self.cfg["pacing"]["verify_after_secs"])
        rows = self.state.pending_older_than(cutoff, int(self.cfg["limits"]["verify_batch"]))
        if not rows:
            return
        res = self.ecx.batch([("getrawtransaction", [txid, 1]) for txid, _, _ in rows])
        lost, kept, confirmed = [], [], []
        for (txid, height, idx), (r, err) in zip(rows, res):
            if err and err.get("code") != -5:
                raise RPCError("getrawtransaction", err.get("code"), err.get("message"))
            if r is None:
                lost.append((txid, height, idx))
            elif r.get("blockhash"):
                confirmed.append(txid)
            else:
                kept.append((txid, height, idx))
        self.state.begin()
        try:
            self.state.pending_mark_verified([t for t, _, _ in kept])
            if confirmed:
                self.state.pending_remove_many(confirmed)
            requeued = self.requeue_from_btc(lost) if lost else 0
            self.state.commit()
        except BaseException:
            self.state.rollback()
            raise
        if lost:
            log.info("pending verifier: %d lost from the ECX mempool re-queued", requeued)

    # ---- pacing ------------------------------------------------------------------------------
    def pace(self):
        """Pause while the ECX mempool is above mempool_fraction × maxmempool."""
        frac = float(self.cfg["pacing"]["mempool_fraction"])
        first = True
        self.pace_at = self.submits
        while not self.stop:
            info, err = self.ecx.try_call("getmempoolinfo")
            if err:
                return
            self.mempool_info = info
            usage, cap = info.get("usage", 0), info.get("maxmempool", 0)
            if not cap or usage <= frac * cap or self.dry_run:
                if self.paused:
                    log.info("pacing: ECX mempool back at %.0f%% of maxmempool; resuming", 100.0 * usage / cap if cap else 0)
                self.paused = False
                return
            if first:
                log.warning("pacing: ECX mempool at %.0f%% of maxmempool (%d/%d bytes); pausing",
                            100.0 * usage / cap, usage, cap)
                first = False
            self.paused = True
            self.write_status()
            time.sleep(float(self.cfg["pacing"]["pace_poll_secs"]))

    # ---- confirmed-set export ------------------------------------------------------------------
    def restore_window(self):
        top = self.state.cursor_top()
        if not top:
            return
        n = int(self.cfg["report"]["confirmed_window"])
        for h in range(max(self.fork_height, top[0] - n + 1), top[0] + 1):
            bh = self.btc.call("getblockhash", h)
            blk = self.btc.call("getblock", bh, 1)
            self.window[h] = (bh, blk["tx"][1:])
        self.write_confirmed_file()

    def write_confirmed_file(self):
        path = self.cfg["report"]["confirmed_file"]
        n = int(self.cfg["report"]["confirmed_window"])
        while len(self.window) > n:
            self.window.popitem(last=False)
        if not self.window:
            return
        top_h = max(self.window)
        lines = [f"# btc_height={top_h} btc_hash={self.window[top_h][0]} updated={int(time.time())}\n"]
        for h in sorted(self.window):
            lines.extend(t + "\n" for t in self.window[h][1])
        atomic_write(path, "".join(lines))

    # ---- reporting -----------------------------------------------------------------------------
    def log_block(self, r):
        tags = json.loads(r["tags"])
        def sub(cls):
            parts = [f"{k.split(':', 1)[1]} {v}" for k, v in sorted(tags.items()) if k.startswith(cls + ":")]
            return f" ({', '.join(parts)})" if parts else ""
        cov = (r["present"] + r["injected"]) / r["ntx"] if r["ntx"] else 1.0
        roll = self.state.rolling_coverage(int(self.cfg["report"]["rolling_blocks"]))
        log.info("block %d tx=%d present=%d injected=%d dead=%d%s retry=%d%s package=%d policy=%d%s conflict=%d%s "
                 "rpc=%d (ecx %d) t=%.1fs coverage=%.1f%% (%d-blk %s)%s", r["height"], r["ntx"], r["present"],
                 r["injected"], r["dead"], sub(DEAD), r["retry"], sub(RETRY), r["package"], r["policy"], sub(POLICY),
                 r["conflict"], sub(CONFLICT), r["rpc"], r["ecx_rpc"], r["secs"], 100 * cov,
                 int(self.cfg["report"]["rolling_blocks"]),
                 f"{100 * roll:.1f}%" if roll is not None else "n/a", "  [dry run]" if self.dry_run else "")

    def write_status(self):
        path = self.cfg["report"]["status_file"]
        if not path:
            return
        top = self.state.cursor_top()
        btc_tip, _ = self.btc.try_call("getblockcount")
        ecx_tip, _ = self.ecx.try_call("getblockcount")
        mp = self.mempool_info or {}
        status = {
            "version": __version__, "network": self.cfg["network"], "fork_height": self.fork_height,
            "mode": self.mode, "dry_run": self.dry_run, "simulate": self.simulate, "paused": self.paused,
            "updated": int(time.time()), "uptime_secs": int(time.time() - self.started),
            "cursor": {"height": top[0], "hash": top[1]} if top else None,
            "btc_tip": btc_tip, "ecx_tip": ecx_tip,
            "ecx_mempool": {k: mp.get(k) for k in ("size", "bytes", "usage", "maxmempool", "mempoolminfee", "minrelaytxfee")},
            "totals": self.state.totals(),
            "rolling_coverage": self.state.rolling_coverage(int(self.cfg["report"]["rolling_blocks"])),
            "retry_queue": self.state.retry_count(), "retry_tags": self.state.retry_tags(),
            "sweeps": {k[6:]: int(v) for k, v in self.state.db.execute("SELECT key, value FROM meta WHERE key LIKE 'sweep_%'")},
            "pending": self.state.pending_count(),
            "package_estimated": int(self.state.get("package_estimated", 0)),
            "dead_cache": len(self.dead), "virtual": len(self.virtual) if self.simulate else None,
            "rpc": {"btc": self.btc.stats(), "ecx": self.ecx.stats()},
            "last_blocks": self.state.block_rows(20),
        }
        atomic_write(path, json.dumps(status, indent=1))


class _RetryTx:
    """A retry-queue row viewed as a BlockTx-alike."""
    __slots__ = ("txid", "hex", "vin", "parents", "fee", "vsize", "idx")

    def __init__(self, row):
        self.txid, self.hex, self.idx = row["txid"], row["hex"], row["idx"]
        self.vin = [tuple(v) for v in json.loads(row["parents"])]
        self.parents = [p for p, _ in self.vin]
        self.fee, self.vsize = row.get("fee"), row.get("vsize")

    feerate = None


class _BlockContext:
    """Lazy, batched presence checks for one block: when a tx needs its presence result we
    check it together with the next `batch` txs that are not (yet) known dead or waiting."""

    def __init__(self, bridge, txs):
        self.bridge, self.txs = bridge, txs
        self.results = {}
        self.block_txids = {t.txid for t in txs}
        self.done = set()

    def presence(self, tx):
        if tx.txid not in self.results:
            self._fill(tx.idx)
        return self.results.get(tx.txid)

    def _fill(self, from_idx):
        b = self.bridge
        want = []
        for t in self.txs[from_idx - 1:]:
            if t.txid in self.results:
                continue
            if any(p in b.dead or p in b.coinbases or p in b.retry_kinds or
                   (p in self.block_txids and p not in self.done) for p, _ in t.vin):
                continue
            want.append(t)
            if len(want) >= b.ecx.batch_size:
                break
        if not want:
            want = [self.txs[from_idx - 1]]
        res = b.ecx.batch([("getrawtransaction", [t.txid, 1]) for t in want])
        for t, (r, err) in zip(want, res):
            if err and err.get("code") != -5:  # anything but "not found" is a node problem, not an absence
                raise RPCError("getrawtransaction", err.get("code"), err.get("message"))
            if r is None:
                self.results[t.txid] = None
            else:
                self.results[t.txid] = "confirmed" if r.get("blockhash") else "mempool"


def atomic_write(path, text):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(prog="garp_replay", description=__doc__.strip().split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", help="TOML config file (see garp_replay.example.toml)")
    p.add_argument("--network", choices=sorted(NETWORKS), help="alphanet|betanet|mainnet|regtest (fork height preset)")
    p.add_argument("--fork-height", type=int, help="first BTC height whose txs must be replayed (required for regtest)")
    g = p.add_argument_group("nodes")
    g.add_argument("--btc-url"); g.add_argument("--btc-cookie"); g.add_argument("--btc-user"); g.add_argument("--btc-password")
    g.add_argument("--ecx-url"); g.add_argument("--ecx-cookie"); g.add_argument("--ecx-user"); g.add_argument("--ecx-password")
    g = p.add_argument_group("run")
    g.add_argument("--mode", choices=["backfill", "live"], help="backfill: cursor->tip then exit; live: keep following")
    g.add_argument("--start-height", type=int, help="first block to process when no cursor exists (default: fork height)")
    g.add_argument("--live-send", action="store_true", help="actually submit (sendrawtransaction). Default is a dry run.")
    g.add_argument("--simulate", action="store_true", help="dry run + virtual overlay (descendants of would-be-injected txs count)")
    g.add_argument("--confirmations-required", type=int)
    g.add_argument("--max-attempts", type=int, help="drop a policy-rejected entry after this many attempts "
                   "(transient retry kinds are never given up)")
    g = p.add_argument_group("pacing")
    g.add_argument("--mempool-fraction", type=float); g.add_argument("--pace-poll-secs", type=float)
    g.add_argument("--retry-interval-secs", type=float); g.add_argument("--verify-after-secs", type=float)
    g.add_argument("--btc-poll-secs", type=float)
    g.add_argument("--dead-cache-entries", type=int)
    g = p.add_argument_group("state and reports")
    g.add_argument("--state", help="sqlite state file")
    g.add_argument("--status-file"); g.add_argument("--confirmed-file", help="export of BTC-confirmed txids (last N blocks)")
    g.add_argument("--confirmed-window", type=int, help="blocks covered by --confirmed-file (default 288)")
    g.add_argument("--log-level")
    g.add_argument("--reset-cursor", action="store_true", help="forget cursor, block stats, retry and pending rows, then exit")
    g.add_argument("--print-status", action="store_true", help="print the status file and exit")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    cfg = load_config(args)
    logging.basicConfig(level=getattr(logging, str(cfg["report"]["log_level"]).upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
    if args.print_status:
        with open(cfg["report"]["status_file"]) as f:
            print(f.read())
        return 0
    if args.reset_cursor:
        st = State(cfg["state"]["path"])
        st.begin(); st.rollback_to(-1); st.commit()
        log.info("state reset: %s", cfg["state"]["path"])
        return 0
    bridge = Bridge(cfg)
    signal.signal(signal.SIGTERM, lambda *_: setattr(bridge, "stop", True))  # finish the current block, then exit
    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("interrupted; state is durable, restart to resume")
        bridge.write_status()
    except TRANSIENT_ERRORS as e:
        log.error("rpc failure: %s (backfill aborted; state is durable, re-run to resume)", e)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
