#!/usr/bin/env python3
"""
Regtest scenarios for garp_replay (design doc D2 §6). Plain python; also importable by pytest.

    python3 tests/test_scenarios.py            # run everything
    python3 tests/test_scenarios.py T1 T5      # a subset

Each scenario starts its own vanilla-Core-29 + eCash-alphanet regtest pair (see regtest_lib.py),
feeds both to a shared height, forks them, drives transactions through the BTC side and runs
garp_replay.py as a subprocess against the pair. Datadirs live under .regtest/<scenario>/.
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from regtest_lib import Pair, RPCError  # noqa: E402

BRIDGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "garp_replay.py")


class Bridge:
    """One garp_replay instance (its own state/status files) against a Pair."""

    def __init__(self, pair, tag="a", live=True, extra=()):
        self.pair, self.live, self.extra = pair, live, list(extra)
        self.dir = os.path.join(pair.root, f"bridge-{tag}")
        os.makedirs(self.dir, exist_ok=True)
        self.state = os.path.join(self.dir, "state.sqlite")
        self.status = os.path.join(self.dir, "status.json")
        self.logfile = os.path.join(self.dir, "run.log")

    def args(self, mode="backfill", live=None, extra=()):
        a = [sys.executable, BRIDGE, "--network", "regtest", "--fork-height", str(self.pair.fork_height),
             "--btc-url", self.pair.van.url, "--ecx-url", self.pair.ecx.url, "--state", self.state,
             "--status-file", self.status, "--mode", mode] + self.extra + list(extra)
        if self.live if live is None else live:
            a.append("--live-send")
        return a

    def start(self, mode="backfill", live=None, extra=(), env=None):
        return subprocess.Popen(self.args(mode, live, extra), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, env={**os.environ, **(env or {})})

    def run(self, mode="backfill", live=None, extra=(), env=None, expect_rc=0, timeout=600):
        p = self.start(mode, live, extra, env)
        out, _ = p.communicate(timeout=timeout)
        with open(self.logfile, "a") as f:
            f.write(out)
        assert p.returncode == expect_rc, f"garp_replay exited {p.returncode} (expected {expect_rc}):\n{out[-3000:]}"
        return out

    def status_json(self):
        with open(self.status) as f:
            return json.load(f)

    def block(self, height):
        rows = [b for b in self.status_json()["last_blocks"] if b["height"] == height]
        assert rows, f"no block row for {height}"
        return rows[0]

    def rpc_calls(self, node, method):
        return self.status_json()["rpc"][node].get(method, {}).get("calls", 0)

    def retry_rows(self):
        db = sqlite3.connect(self.state)
        rows = db.execute("SELECT txid, height, kind, tag, attempts FROM retry ORDER BY height, idx").fetchall()
        db.close()
        return rows

    def cursor(self):
        return (self.status_json().get("cursor") or {}).get("height")


# --- helpers ------------------------------------------------------------------------------------

def plain_txs(pair, n, coins=None):
    """n independent single-input spends of fan-out coins, broadcast to the BTC node. -> [txid]"""
    coins = coins if coins is not None else pair.coins(max_amount=1)
    assert len(coins) >= n, f"need {n} small coins, have {len(coins)}"
    out = []
    for c in coins[:n]:
        txid, hx, _ = pair.spend_coin(c)
        pair.van.rpc("sendrawtransaction", hx)
        out.append(txid)
    return out


def chain_txs(pair, coin, n, start_amount=None):
    """n-deep chain spending `coin`, each to a fresh address, broadcast to BTC. -> [(txid, amount)]"""
    prev, pv, amt = coin["txid"], coin["vout"], float(start_amount or coin["amount"])
    out = []
    for _ in range(n):
        amt = round(amt - 0.0001, 8)
        txid, hx = pair.make_tx([(prev, pv, amt + 0.0001)], {pair.van.rpc("getnewaddress"): amt})
        pair.van.rpc("sendrawtransaction", hx)
        out.append((txid, amt))
        prev, pv = txid, 0
    return out


def ecx_has_all(pair, txids):
    return all(pair.ecx.has_tx(t) for t in txids)


def tags(row):
    return json.loads(row["tags"])


# --- scenarios ----------------------------------------------------------------------------------

def t01_plain(port):
    """T1: 50 plain txs in one BTC block -> all in the ECX mempool; rerun = zero submits;
    a fresh second instance sees present=50."""
    pair = Pair("t01", portbase=port).start().setup()
    try:
        txids = plain_txs(pair, 50)
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        assert pair.ecx.mempool() == set(txids), "ECX mempool != the 50 txs"
        row = b.block(h)
        assert (row["ntx"], row["injected"], row["present"], row["dead"], row["retry"]) == (50, 50, 0, 0, 0), row
        n1 = b.rpc_calls("ecx", "sendrawtransaction")
        assert n1 == 50, n1
        b.run()  # same state: cursor is at the tip, nothing to do
        assert b.rpc_calls("ecx", "sendrawtransaction") == 0, "rerun submitted something"
        assert b.cursor() == h
        b2 = Bridge(pair, "b")  # fresh state: everything is already there
        b2.run()
        row2 = b2.block(h)
        assert (row2["present"], row2["injected"]) == (50, 0), row2
        assert b2.rpc_calls("ecx", "sendrawtransaction") == 0
        assert pair.ecx.mempool() == set(txids)
        return f"50 injected; rerun 0 submits; second instance present=50 (ecx_rpc={row2['ecx_rpc']})"
    finally:
        pair.stop()


def t02_cluster(port):
    """T2: 30-deep chain across 3 BTC blocks, ECX -limitclustercount=10 and not mining: the tail
    is RETRY(cluster)/child-of-retry and drains one ECX block at a time."""
    pair = Pair("t02", ecx_extra=["-limitclustercount=10"], portbase=port).start().setup()
    try:
        coin = pair.coins(min_amount=1)[-1]  # a large coin
        chain, heights = [], []
        amt = None
        for _ in range(3):
            part = chain_txs(pair, coin if not chain else {"txid": chain[-1][0], "vout": 0, "amount": chain[-1][1]}, 10)
            chain += part
            pair.van.mine(1)
            heights.append(pair.van.height())
        txids = [t for t, _ in chain]
        b = Bridge(pair, "a")
        b.run()
        r1, r2, r3 = (b.block(h) for h in heights)
        assert r1["injected"] == 10, r1
        assert r2["retry"] == 10 and tags(r2).get("retry:cluster") == 1 and tags(r2).get("retry:child-of-retry") == 9, r2
        assert r3["retry"] == 10 and tags(r3).get("retry:child-of-retry") == 10 and r3["ecx_rpc"] == 0, r3
        assert pair.ecx.mempool() == set(txids[:10])
        assert len(b.retry_rows()) == 20
        pair.ecx.mine(1)  # confirms the first 10 on ECX
        b.run()
        assert pair.ecx.mempool() == set(txids[10:20]), "second batch did not drain"
        assert len(b.retry_rows()) == 10 and b.retry_rows()[0][3] == "cluster"
        pair.ecx.mine(1)
        b.run()
        assert pair.ecx.mempool() == set(txids[20:30]), "third batch did not drain"
        assert b.retry_rows() == []
        assert ecx_has_all(pair, txids)
        sw = b.status_json()["sweeps"]
        return f"10 injected, 20 queued (cluster 1 + child-of-retry 19), drained over 2 ECX blocks; sweeps={sw}"
    finally:
        pair.stop()


def t03_coinbase(port):
    """T3: spend of a post-fork BTC coinbase + 3 descendants -> dead(coinbase)=1, dead(ancestor)=3,
    zero ECX RPCs for the whole block."""
    pair = Pair("t03", portbase=port).start().setup()
    try:
        addr = pair.van.rpc("getnewaddress")
        cbblock = pair.van.mine(1, addr)[0]
        cbtx = pair.van.rpc("getblock", cbblock, 2)["tx"][0]
        cb, cbval = cbtx["txid"], cbtx["vout"][0]["value"]  # regtest subsidy halves every 150 blocks
        pair.van.mine(100, addr)  # mature it
        chain = chain_txs(pair, {"txid": cb, "vout": 0, "amount": cbval}, 4)
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        row = b.block(h)
        assert (row["ntx"], row["dead"], row["dead_coinbase"], row["dead_ancestor"]) == (4, 4, 1, 3), row
        assert row["ecx_rpc"] == 0, row
        assert b.rpc_calls("ecx", "sendrawtransaction") == 0
        assert pair.ecx.mempool() == set()
        assert b.retry_rows() == []
        return f"dead=4 (coinbase 1, ancestor 3), ecx_rpc=0, submits=0 over {len(chain)} txs"
    finally:
        pair.stop()


def t05_rbf(port):
    """T5: A sits in the ECX mempool (gossip), BTC confirms A' -> A' replaces A on ECX.
    Variant: ECX mined B before BTC confirmed B' -> conflict(split) counted, never re-submitted."""
    pair = Pair("t05", portbase=port).start().setup()
    try:
        coins = pair.coins(max_amount=1)
        x, y = coins[0], coins[1]
        A, hA = pair.make_tx([(x["txid"], x["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.4999})
        A2, hA2 = pair.make_tx([(x["txid"], x["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.499})
        pair.ecx.rpc("sendrawtransaction", hA)          # "gossip" delivered A to ECX
        pair.van.rpc("sendrawtransaction", hA2)         # BTC confirms the fee bump
        pair.van.mine(1)
        h1 = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        mp = pair.ecx.mempool()
        assert A2 in mp and A not in mp, f"A' should have replaced A: {mp}"
        assert b.block(h1)["injected"] == 1, b.block(h1)
        # variant: ECX already mined B
        B, hB = pair.make_tx([(y["txid"], y["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.4999})
        B2, hB2 = pair.make_tx([(y["txid"], y["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.499})
        pair.ecx.rpc("sendrawtransaction", hB)
        pair.ecx.mine(1)
        assert pair.ecx.confirmed(B)
        pair.van.rpc("sendrawtransaction", hB2)
        pair.van.mine(1)
        h2 = pair.van.height()
        n0 = b.rpc_calls("ecx", "sendrawtransaction")
        b.run()
        row = b.block(h2)
        assert (row["conflict"], row["conflict_split"], row["injected"], row["dead"]) == (1, 1, 0, 0), row
        assert not pair.ecx.has_tx(B2)
        assert b.retry_rows() == []
        assert b.rpc_calls("ecx", "sendrawtransaction") == 1, "exactly one attempt classifies the conflict"
        b.run()  # rerun: nothing re-submitted
        assert b.rpc_calls("ecx", "sendrawtransaction") == 0
        pair.ecx.mine(1)
        b.run()  # even after an ECX block (sweep) nothing is re-submitted
        assert b.rpc_calls("ecx", "sendrawtransaction") == 0
        return "A' replaced A on ECX; ECX-mined B vs BTC B': conflict(split)=1, one attempt, no re-submission"
    finally:
        pair.stop()


def t08_csv(port):
    """T8: CSV child (nSequence=10) confirmed on BTC 10 blocks after its parent; on ECX the parent
    confirms later, so the child is RETRY(bip68) until ECX has mined enough blocks."""
    pair = Pair("t08", portbase=port).start().setup()
    try:
        c = pair.coins(max_amount=1)[0]
        P, hP = pair.make_tx([(c["txid"], c["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.4999})
        pair.van.rpc("sendrawtransaction", hP)
        pair.van.mine(1)
        hp = pair.van.height()
        pair.van.mine(10)
        C, hC = pair.make_tx([(P, 0, 0.4999)], {pair.van.rpc("getnewaddress"): 0.4998}, sequence=10)
        pair.van.rpc("sendrawtransaction", hC)
        pair.van.mine(1)
        hc = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        assert b.block(hp)["injected"] == 1
        row = b.block(hc)
        assert row["retry"] == 1 and tags(row).get("retry:bip68") == 1, row
        assert pair.ecx.mempool() == {P}
        pair.ecx.mine(1)  # P confirmed on ECX at height e
        e = pair.ecx.height()
        drained_at = None
        for step in range(12):
            b.run()
            if C in pair.ecx.mempool():
                drained_at = pair.ecx.height() - e
                break
            rows = b.retry_rows()
            assert len(rows) == 1 and rows[0][3] == "bip68", rows
            pair.ecx.mine(1)
        assert drained_at is not None, "C never drained"
        assert drained_at >= 8, f"C accepted too early: {drained_at} ECX blocks after its parent"
        assert b.retry_rows() == []
        return f"child RETRY(bip68) until ECX had mined {drained_at} blocks past the parent, then injected"
    finally:
        pair.stop()


def t10_crash_and_concurrency(port):
    """T10: kill -9 mid-block then restart -> full coverage, cursor only advances on completion,
    no duplicates; two instances on the same block concurrently -> identical coverage."""
    pair = Pair("t10", portbase=port).start()
    pair.setup(fanout=450, fanout_value=0.1)
    try:
        coins = pair.coins(max_amount=0.5)
        txids = plain_txs(pair, 200, coins[:200])
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        out = b.run(env={"GARP_CRASH_AFTER_SUBMITS": "100"}, expect_rc=137)
        assert "simulating a crash" in out
        assert b.cursor() is None, "cursor must not advance for an unfinished block"
        n_before = len(pair.ecx.mempool())
        assert 90 <= n_before <= 100, n_before
        b.run()
        row = b.block(h)
        assert row["present"] + row["injected"] == 200 and row["present"] == n_before, row
        assert pair.ecx.mempool() == set(txids)
        assert b.retry_rows() == [] and b.cursor() == h
        # two instances at once on a fresh block
        txids2 = plain_txs(pair, 200, coins[200:400])
        pair.van.mine(1)
        h2 = pair.van.height()
        b1, b2 = Bridge(pair, "c1"), Bridge(pair, "c2")
        p1, p2 = b1.start(), b2.start()
        o1, _ = p1.communicate(timeout=600)
        o2, _ = p2.communicate(timeout=600)
        assert p1.returncode == 0 and p2.returncode == 0, (o1[-2000:], o2[-2000:])
        r1, r2 = b1.block(h2), b2.block(h2)
        assert r1["present"] + r1["injected"] == 200 and r2["present"] + r2["injected"] == 200, (r1, r2)
        assert r1["dead"] == r2["dead"] == r1["retry"] == r2["retry"] == r1["conflict"] == r2["conflict"] == 0
        assert pair.ecx.mempool() == set(txids) | set(txids2)
        assert b1.retry_rows() == [] and b2.retry_rows() == []
        return (f"crash after 100 submits: cursor unchanged, {n_before} in mempool; restart -> present={row['present']} "
                f"injected={row['injected']}; concurrent: c1 injected={r1['injected']} c2 injected={r2['injected']}, "
                f"mempool exactly 400")
    finally:
        pair.stop()


def t12_dry_run(port):
    """T12: the default (dry run) leaves the ECX mempool untouched and reports what live would do."""
    pair = Pair("t12", portbase=port).start().setup()
    try:
        txids = plain_txs(pair, 50)
        pair.van.mine(1)
        h = pair.van.height()
        d = Bridge(pair, "dry", live=False)
        d.run()
        assert pair.ecx.mempool() == set(), "dry run touched the ECX mempool"
        rd = d.block(h)
        assert (rd["injected"], rd["present"]) == (50, 0), rd
        s = d.status_json()
        assert s["dry_run"] is True and d.rpc_calls("ecx", "sendrawtransaction") == 0
        assert d.rpc_calls("ecx", "testmempoolaccept") == 50
        assert s["pending"] == 50, "dry run remembers would-be injections in pending (restart consistency)"
        live = Bridge(pair, "live")
        live.run()
        rl = live.block(h)
        keys = ("ntx", "present", "injected", "dead", "retry", "package", "policy", "conflict")
        assert tuple(rd[k] for k in keys) == tuple(rl[k] for k in keys), (rd, rl)
        assert pair.ecx.mempool() == set(txids)
        return "dry run: mempool unchanged, 50 testmempoolaccept, 0 sendrawtransaction; live run identical counts"
    finally:
        pair.stop()


def t13_confirmed_file(port):
    """--confirmed-file: header line + exactly the non-coinbase txids of the last N blocks; atomic;
    restored from the BTC node after a restart."""
    pair = Pair("t13", portbase=port).start().setup()
    try:
        coins = pair.coins(max_amount=1)
        per_block, blocks = 3, []
        for i in range(5):
            txids = plain_txs(pair, per_block, coins[i * per_block:(i + 1) * per_block])
            pair.van.mine(1)
            blocks.append((pair.van.height(), pair.van.tip(), set(txids)))
        path = os.path.join(pair.root, "confirmed.txt")
        b = Bridge(pair, "a", extra=["--confirmed-file", path, "--confirmed-window", "3"])
        b.run()
        want = set().union(*(s for _, _, s in blocks[-3:]))

        def check():
            with open(path) as f:
                lines = f.read().splitlines()
            head = lines[0].split()
            assert head[0] == "#" and head[1] == f"btc_height={blocks[-1][0]}" and head[2] == f"btc_hash={blocks[-1][1]}", lines[0]
            assert head[3].startswith("updated=") and int(head[3][8:]) > 0
            body = lines[1:]
            assert len(body) == len(set(body)) == 9, len(body)
            assert set(body) == want, "body != txids of the last 3 blocks"
            assert all(len(x) == 64 for x in body)
        check()
        assert not os.path.exists(path + f".tmp.{os.getpid()}")
        os.remove(path)
        b.run()  # cursor at tip: nothing processed, window restored from the BTC node
        check()
        return "header ok; body = exactly the 9 txids of the last 3 blocks (no coinbases); restored after restart"
    finally:
        pair.stop()


def t06_reorg(port):
    """T6 (extra): BTC reorg with a different tx set -> fork point found, cursor rolled back,
    re-walk injects the new txs, stale retry rows are gone."""
    pair = Pair("t06", portbase=port).start().setup()
    try:
        coins = pair.coins(max_amount=1)
        old = plain_txs(pair, 5, coins[:5])
        # a child of a queued (retry) tx so that a stale retry row exists: make it non-final on ECX
        c = coins[5]
        L, hL = pair.make_tx([(c["txid"], c["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.4999}, locktime=5000)
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        assert pair.ecx.mempool() == set(old)
        # reorg: replace the last block with two blocks containing 3 of the old txs and 4 new ones
        stale = pair.van.tip()
        pair.van.rpc("invalidateblock", stale)
        assert pair.van.height() == h - 1
        keep, dropped = old[:3], old[3:]
        pair.van.rpc("generateblock", pair.van.rpc("getnewaddress"), keep)
        new = plain_txs(pair, 4, coins[6:10])
        pair.van.mine(1)
        assert pair.van.height() == h + 1 and pair.van.rpc("getblockhash", h) != stale
        out = b.run()
        assert "BTC reorg" in out
        assert b.cursor() == h + 1
        rows = {r["height"]: r for r in b.status_json()["last_blocks"]}
        assert rows[h]["present"] == 3 and rows[h + 1]["injected"] == 4, (rows[h], rows[h + 1])
        assert set(new) <= pair.ecx.mempool()
        assert b.retry_rows() == [], b.retry_rows()  # the non-final L was in the orphaned block
        return "fork point found, cursor rolled back one block, 3 present + 4 injected on the new branch, stale retry row gone"
    finally:
        pair.stop()


def t07_package(port):
    """T7 (extra): v3 zero-fee parent + CPFP child (submitpackage on BTC) -> parent PACKAGE(minrelay),
    child completes the package via submitpackage on ECX; both in the ECX mempool.
    (v3 because Core 29's submitpackage refuses a v2 zero-fee parent; ECX 31 accepts both.)"""
    pair = Pair("t07", portbase=port).start().setup()
    try:
        c = pair.coins(max_amount=1)[0]
        P, hP = pair.make_tx([(c["txid"], c["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.5}, version=3)
        C, hC = pair.make_tx([(P, 0, 0.5)], {pair.van.rpc("getnewaddress"): 0.49}, parents={P: hP}, version=3)
        r = pair.van.rpc("submitpackage", [hP, hC])
        assert r["package_msg"] == "success", r
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        row = b.block(h)
        assert row["package"] == 1 and row["injected"] == 1 and tags(row).get("package:minrelay") == 1, row
        assert pair.ecx.mempool() == {P, C}
        assert b.retry_rows() == []
        assert b.rpc_calls("ecx", "submitpackage") == 1
        return "parent PACKAGE(minrelay), child triggered submitpackage, both in ECX mempool"
    finally:
        pair.stop()


def t14_simulate(port):
    """T14 (extra): parent + child in one BTC block. Plain dry run reports the child as
    retry:dry-run-parent (cannot be evaluated without injecting the parent); --simulate counts it
    as injected via the virtual overlay; neither touches ECX; live injects both."""
    pair = Pair("t14", portbase=port).start().setup()
    try:
        coin = pair.coins(max_amount=1)[0]
        chain = chain_txs(pair, coin, 2)
        pair.van.mine(1)
        h = pair.van.height()
        d = Bridge(pair, "dry", live=False)
        d.run()
        rd = d.block(h)
        assert rd["injected"] == 1 and rd["retry"] == 1 and tags(rd).get("retry:dry-run-parent") == 1, rd
        assert d.retry_rows() == [], "dry-run-parent must not be queued"
        s = Bridge(pair, "sim", live=False, extra=["--simulate"])
        s.run()
        rs = s.block(h)
        assert rs["injected"] == 2 and rs["retry"] == 0, rs
        assert s.status_json()["simulate"] is True and s.status_json()["virtual"] == 2
        assert pair.ecx.mempool() == set()
        live = Bridge(pair, "live")
        live.run()
        assert live.block(h)["injected"] == 2 and pair.ecx.mempool() == {t for t, _ in chain}
        return "dry run: 1 injected + 1 retry:dry-run-parent (not queued); simulate: 2 injected (virtual); live: 2"
    finally:
        pair.stop()


def t15_live_mode(port):
    """T15 (extra): --mode live as a long-running process: a new BTC block is picked up via
    waitfornewblock, an ECX block moves injected txs from pending to confirmed."""
    pair = Pair("t15", portbase=port).start().setup()
    try:
        coins = pair.coins(max_amount=1)
        first = plain_txs(pair, 5, coins[:5])
        pair.van.mine(1)
        b = Bridge(pair, "live", extra=["--btc-poll-secs", "2", "--retry-interval-secs", "5"])
        proc = b.start(mode="live")
        try:
            def wait_for(pred, secs=60, what="condition"):
                t0 = time.time()
                while time.time() - t0 < secs:
                    if proc.poll() is not None:
                        raise AssertionError(f"bridge exited early: {proc.stdout.read()[-2000:]}")
                    try:
                        if pred():
                            return
                    except (OSError, ValueError, KeyError):
                        pass
                    time.sleep(0.5)
                raise AssertionError(f"timeout waiting for {what}")
            wait_for(lambda: pair.ecx.mempool() == set(first), what="catch-up injection")
            second = plain_txs(pair, 5, coins[5:10])
            pair.van.mine(1)
            h2 = pair.van.height()
            wait_for(lambda: set(second) <= pair.ecx.mempool(), what="live block injection")
            wait_for(lambda: b.cursor() == h2 and b.status_json()["pending"] == 10, what="status pending=10")
            pair.ecx.mine(1)
            wait_for(lambda: b.status_json()["pending"] == 0, what="pending confirmed after ECX block")
            assert pair.ecx.mempool() == set()
            return "live: catch-up 5, new BTC block 5 within seconds, ECX block cleared pending 10 -> 0"
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
            with open(b.logfile, "a") as f:
                f.write(proc.stdout.read())
    finally:
        pair.stop()


# --- review fixes (0.1.1) --------------------------------------------------------------------------

def wait_for(pred, proc=None, secs=60, what="condition"):
    t0 = time.time()
    while time.time() - t0 < secs:
        if proc is not None and proc.poll() is not None:
            raise AssertionError(f"bridge exited early: {proc.stdout.read()[-3000:]}")
        try:
            if pred():
                return
        except (OSError, ValueError, KeyError, RPCError, AssertionError):
            pass
        time.sleep(0.5)
    raise AssertionError(f"timeout waiting for {what}")


def stop_live(proc, b):
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
    out = proc.stdout.read()
    with open(b.logfile, "a") as f:
        f.write(out)
    return out


def t16_evicted_parent(port):
    """T16 (review blocker): P injected; ECX restarted without persistmempool (mempool lost) while the live
    bridge runs; BTC then confirms C1 (spends P:0) and later C2 (spends P:1). Neither child may be classed
    dead; the bridge must survive the ECX restart (-28 warm-up), re-queue P and deliver P, C1, C2."""
    pair = Pair("t16", portbase=port).start().setup()
    try:
        c = pair.coins(max_amount=1)[0]
        a1, a2 = pair.van.rpc("getnewaddress"), pair.van.rpc("getnewaddress")
        P, hP = pair.make_tx([(c["txid"], c["vout"], 0.5)], {a1: 0.2, a2: 0.2999})
        pair.van.rpc("sendrawtransaction", hP)
        pair.van.mine(1)
        h1 = pair.van.height()
        b = Bridge(pair, "live", extra=["--btc-poll-secs", "1", "--retry-interval-secs", "3"])
        proc = b.start(mode="live")
        try:
            wait_for(lambda: pair.ecx.mempool() == {P}, proc, what="P injected")
            wait_for(lambda: b.cursor() == h1, proc, what="cursor h1")
            # ECX loses its mempool: graceful stop (chainstate flushed), restart wallet-less so nothing
            # re-broadcasts P, and without persistmempool. The bridge keeps polling through the warm-up.
            pair.ecx.stop()
            pair.ecx.extra = ["-persistmempool=0", "-disablewallet"]
            pair.ecx.start()
            assert pair.ecx.mempool() == set()
            time.sleep(2)
            assert proc.poll() is None, "bridge died during the ECX restart"
            C1, hC1 = pair.make_tx([(P, 0, 0.2)], {pair.van.rpc("getnewaddress"): 0.1999})
            pair.van.rpc("sendrawtransaction", hC1)
            pair.van.mine(1)
            h2 = pair.van.height()
            wait_for(lambda: b.cursor() == h2, proc, what="cursor h2")
            row2 = b.block(h2)
            assert row2["dead"] == 0 and row2["retry"] == 1 and tags(row2).get("retry:child-of-retry") == 1, row2
            kinds = {r[0]: r[3] for r in b.retry_rows()}
            assert kinds.get(P) == "evicted" and kinds.get(C1) == "child-of-retry", kinds
            # the periodic sweep (3 s) re-injects P then C1, no ECX block needed
            wait_for(lambda: pair.ecx.mempool() == {P, C1}, proc, secs=30, what="P and C1 re-injected by the sweep")
            wait_for(lambda: b.retry_rows() == [], proc, what="queue drained")
            C2, hC2 = pair.make_tx([(P, 1, 0.2999)], {pair.van.rpc("getnewaddress"): 0.2998})
            pair.van.rpc("sendrawtransaction", hC2)
            pair.van.mine(1)
            h3 = pair.van.height()
            wait_for(lambda: b.cursor() == h3, proc, what="cursor h3")
            row3 = b.block(h3)
            assert row3["injected"] == 1 and row3["dead"] == 0, row3
            assert pair.ecx.mempool() == {P, C1, C2}
            assert b.status_json()["retry_queue"] == 0
        finally:
            out = stop_live(proc, b)
        assert "re-queueing it" in out, out[-2000:]
        return "ECX mempool loss: P re-queued (evicted), C1 child-of-retry, sweep delivered P+C1, C2 injected; bridge survived the restart"
    finally:
        pair.stop()


def t17_no_give_up_on_transient(port):
    """T17 (review major): --max-attempts must not apply to transient kinds. 12-deep chain, ECX cluster limit 10,
    --max-attempts 2, three backfill runs: nothing is given up; once ECX mines, the tail is injected."""
    pair = Pair("t17", ecx_extra=["-limitclustercount=10"], portbase=port).start().setup()
    try:
        coin = pair.coins(min_amount=1)[-1]
        txids = [t for t, _ in chain_txs(pair, coin, 12)]
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a", extra=["--max-attempts", "2"])
        outs = [b.run() for _ in range(4)]
        assert not any("giving up" in o or "dropping policy" in o for o in outs)
        rows = b.retry_rows()
        assert [r[0] for r in rows] == txids[10:] and rows[0][3] == "cluster" and rows[1][3] == "child-of-retry", rows
        assert rows[0][4] >= 3, "the cluster-limited tx must keep being retried"
        pair.ecx.mine(1)
        b.run()
        assert pair.ecx.mempool() == set(txids[10:]), pair.ecx.mempool()
        assert b.retry_rows() == []
        return f"cluster tail retried {rows[0][4]} times under --max-attempts 2, never dropped; injected after ECX mined"
    finally:
        pair.stop()


def t18_ecx_node_checks(port):
    """T18 (review major): an ECX node in IBD / below the last shared block is refused in backfill (nothing
    recorded) and waited for in live mode, which then proceeds once the real node is back."""
    pair = Pair("t18", portbase=port).start().setup()
    try:
        plain_txs(pair, 3)
        pair.van.mine(1)
        h = pair.van.height()
        pair.ecx.stop()
        good = pair.ecx.datadir + ".good"
        os.rename(pair.ecx.datadir, good)
        pair.ecx.start()  # fresh datadir: genesis only -> IBD, height 0
        b = Bridge(pair, "a")
        out = b.run(expect_rc=1)
        assert "initial block download" in out and "below the last shared block" in out, out[-1500:]
        pair.ecx.rpc("createwallet", "w")
        pair.ecx.mine(1)  # out of IBD but still below fork-1 (and on another chain)
        out = b.run(expect_rc=1)
        assert "below the last shared block" in out and "initial block download" not in out, out[-1500:]
        assert not os.path.exists(b.status) or b.cursor() is None
        assert b.retry_rows() == []
        # live mode waits, then proceeds when the real node is back
        bl = Bridge(pair, "live", extra=["--btc-poll-secs", "1"])
        proc = bl.start(mode="live")
        try:
            time.sleep(4)
            assert proc.poll() is None, "live bridge must wait, not exit"
            pair.ecx.stop()
            import shutil
            shutil.rmtree(pair.ecx.datadir)
            os.rename(good, pair.ecx.datadir)
            pair.ecx.start()
            wait_for(lambda: bl.cursor() == h, proc, what="block processed after the node came back")
            assert bl.block(h)["injected"] == 3
        finally:
            out = stop_live(proc, bl)
        assert "waiting for nodes" in out
        return "IBD/below-fork ECX refused in backfill (nothing recorded); live mode waited, then injected 3"
    finally:
        pair.stop()


def t19_cpfp_dry_run(port):
    """T19 (review major): zero-fee v2 parent P (2 outputs) + CPFP child C1 + sibling C2, mined out-of-band on
    BTC. testmempoolaccept cannot evaluate a CPFP package (members judged on their own fee), so the dry modes
    estimate the package feerate from BTC fee data: simulate reports what live does."""
    pair = Pair("t19", portbase=port).start().setup()
    try:
        c = pair.coins(max_amount=1)[0]
        a1, a2 = pair.van.rpc("getnewaddress"), pair.van.rpc("getnewaddress")
        P, hP = pair.make_tx([(c["txid"], c["vout"], 0.5)], {a1: 0.25, a2: 0.25})  # zero fee
        C1, hC1 = pair.make_tx([(P, 0, 0.25)], {pair.van.rpc("getnewaddress"): 0.24}, parents={P: hP})
        C2, hC2 = pair.make_tx([(P, 1, 0.25)], {pair.van.rpc("getnewaddress"): 0.2499}, parents={P: hP})
        pair.van.rpc("generateblock", pair.van.rpc("getnewaddress"), [hP, hC1, hC2])
        h = pair.van.height()
        sim = Bridge(pair, "sim", live=False, extra=["--simulate"])
        sim.run()
        rs = sim.block(h)
        assert (rs["injected"], rs["package"], rs["retry"], rs["dead"]) == (2, 1, 0, 0), rs
        assert sim.status_json()["package_estimated"] == 1 and sim.retry_rows() == []
        dry = Bridge(pair, "dry", live=False)
        dry.run()
        rd = dry.block(h)
        assert (rd["injected"], rd["package"], rd["dead"]) == (1, 1, 0) and tags(rd).get("retry:dry-run-parent") == 1, rd
        assert pair.ecx.mempool() == set()
        live = Bridge(pair, "live")
        live.run()
        rl = live.block(h)
        assert (rl["injected"], rl["package"]) == (2, 1) and pair.ecx.mempool() == {P, C1, C2}, rl
        return "simulate: injected=2 package=1 (1 estimated) = live; dry: 1 injected + dry-run-parent; ECX untouched until live"
    finally:
        pair.stop()


def t20_pacing_calls(port):
    """T20 (review minor): the every-500-submits pacing check fires once per 500 submissions, not on every
    non-submitting tx after the 500th. Block = 500 plain txs followed by 21 post-fork-coinbase-tainted (dead) txs."""
    pair = Pair("t20", portbase=port).start()
    pair.setup(fanout=520, fanout_value=0.05)
    try:
        addr = pair.van.rpc("getnewaddress")
        cbblock = pair.van.mine(1, addr)[0]
        cbtx = pair.van.rpc("getblock", cbblock, 2)["tx"][0]
        cb, cbval = cbtx["txid"], cbtx["vout"][0]["value"]
        pair.van.mine(100, addr)
        coins = pair.coins(max_amount=0.5)
        plain = plain_txs(pair, 500, coins[:500])
        each = round((float(cbval) - 0.001) / 20, 8)
        F, hF = pair.make_tx([(cb, 0, float(cbval))], {pair.van.rpc("getnewaddress"): each for _ in range(20)})
        dead, dead_hex = [F], [hF]
        for i in range(20):
            t, hx = pair.make_tx([(F, i, each)], {pair.van.rpc("getnewaddress"): round(each - 0.0001, 8)}, parents={F: hF})
            dead.append(t)
            dead_hex.append(hx)
        pair.van.rpc("generateblock", addr, plain + dead_hex)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        row = b.block(h)
        assert (row["injected"], row["dead"]) == (500, 21), row
        mpi = b.rpc_calls("ecx", "getmempoolinfo")
        nblocks = h - pair.fork_height + 1  # one pacing check per BTC block walked (101 empty maturity blocks + this one)
        assert mpi == nblocks + 1, f"getmempoolinfo called {mpi} times (expected {nblocks} per-block + 1 at submit #500)"
        return f"500 injected + 21 dead over {nblocks} blocks: getmempoolinfo calls = {mpi} (= blocks + 1)"
    finally:
        pair.stop()


def t21_btc_tip_below_cursor(port):
    """T21 (review major, A13): BTC tip below the cursor with no replacement block yet -> wait (exit 0 in
    backfill, cursor untouched, no rollback); once a replacement block exists the reorg is handled."""
    pair = Pair("t21", portbase=port).start().setup()
    try:
        plain_txs(pair, 3)
        pair.van.mine(1)
        b = Bridge(pair, "a")
        b.run()
        h = b.cursor()
        pair.van.rpc("invalidateblock", pair.van.tip())
        assert pair.van.height() == h - 1
        out = b.run()
        assert "below the cursor" in out and "BTC reorg" not in out and b.cursor() == h, out[-1500:]
        pair.van.mine(1)
        out2 = b.run()
        assert "BTC reorg" in out2 and b.cursor() == h and b.block(h)["present"] == 3, out2[-1500:]
        return "tip below cursor: waited (rc 0, cursor kept); replacement block: reorg handled, 3 present"
    finally:
        pair.stop()


def t22_ecx_reorg_walk_bounded(port):
    """T22 (review major, A4b): after a 1-block ECX reorg the follower walks a bounded number of headers
    (new blocks + ECX_REORG_DEPTH), not the whole chain; ECX blocks mined between backfill runs are walked."""
    pair = Pair("t22", portbase=port).start().setup()
    try:
        plain_txs(pair, 3)
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "live", extra=["--btc-poll-secs", "1", "--retry-interval-secs", "600"])
        proc = b.start(mode="live")
        try:
            def calls(m):
                return b.status_json()["rpc"]["ecx"].get(m, {}).get("calls", 0)
            wait_for(lambda: b.cursor() == h, proc, what="cursor")
            pair.ecx.mine(1)
            wait_for(lambda: calls("getblockheader") >= 1, proc, what="first ECX block followed")
            time.sleep(2)
            h0, b0 = calls("getblockheader"), calls("getblock")
            old_tip, eh = pair.ecx.tip(), pair.ecx.height()
            pair.ecx.rpc("invalidateblock", old_tip)
            pair.ecx.mine(2)
            assert pair.ecx.height() == eh + 1
            wait_for(lambda: calls("getblockheader") > h0, proc, what="reorg followed")
            time.sleep(4)
            dh, db = calls("getblockheader") - h0, calls("getblock") - b0
            assert dh <= 8 and db <= 8, f"walked {dh} headers / {db} blocks after a 1-block reorg on a {eh + 1}-block chain"
        finally:
            out = stop_live(proc, b)
        assert "ecx reorg" in out
        # backfill: ECX blocks mined between two runs are walked (ecx_tip persisted from the first run on)
        b2 = Bridge(pair, "b")
        b2.run()
        pair.ecx.mine(1)
        out = b2.run()
        assert "ecx block(s): 1 new" in out, out[-1500:]
        return f"1-block ECX reorg: +{dh} getblockheader, +{db} getblock; backfill rerun saw '1 new' ECX block"
    finally:
        pair.stop()


def t23_replacement_failed(port):
    """T23 (review minor, A1b): v31 feerate-diagram RBF loss ('replacement-failed') is conflict:rbf-loss,
    counted once and never re-submitted."""
    pair = Pair("t23", portbase=port).start().setup()
    try:
        coins = pair.coins(max_amount=1)
        x, others = coins[0], coins[1:21]
        A, hA = pair.make_tx([(x["txid"], x["vout"], 0.5)], {pair.van.rpc("getnewaddress"): 0.499})
        inputs = [(x["txid"], x["vout"], 0.5)] + [(o["txid"], o["vout"], float(o["amount"])) for o in others]
        total = sum(a for _, _, a in inputs)
        A2, hA2 = pair.make_tx(inputs, {pair.van.rpc("getnewaddress"): round(total - 0.002, 8)})
        pair.ecx.rpc("sendrawtransaction", hA)
        pair.van.rpc("sendrawtransaction", hA2)
        pair.van.mine(1)
        h = pair.van.height()
        b = Bridge(pair, "a")
        b.run()
        row = b.block(h)
        assert (row["conflict"], row["policy"]) == (1, 0) and tags(row).get("conflict:rbf-loss") == 1, row
        assert b.retry_rows() == []
        b.run()
        assert b.rpc_calls("ecx", "sendrawtransaction") == 0
        return "replacement-failed -> conflict:rbf-loss, no retry row, no re-submission"
    finally:
        pair.stop()


def t24_units(port):
    """T24 (review minors, no nodes): batch-level error object -> RPCError; IncompleteRead is retried; a sweep
    submit that comes back PRESENT dequeues the row and tracks it as pending."""
    import http.client
    import tempfile
    import garp_replay as g
    from regtest_lib import ROOT
    os.makedirs(ROOT, exist_ok=True)
    root = tempfile.mkdtemp(prefix="t24-", dir=ROOT)
    rpc = g.RPC("x", "http://127.0.0.1:1/")
    rpc._post = lambda payload: {"result": None, "error": {"code": -32700, "message": "Parse error"}, "id": None}
    try:
        rpc.batch([("getrawtransaction", ["00" * 32, 1])])
        raise AssertionError("batch accepted a non-list response")
    except g.RPCError as e:
        assert e.code == -32700
    # IncompleteRead on the first attempt, success on the second
    calls = []
    class _R:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b'{"result": 7, "error": null, "id": "garp"}'
    def fake_urlopen(req, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            raise http.client.IncompleteRead(b"")
        return _R()
    g.urllib.request.urlopen, saved = fake_urlopen, g.urllib.request.urlopen
    g.time.sleep, saved_sleep = (lambda s: None), g.time.sleep
    try:
        assert g.RPC("y", "http://127.0.0.1:1/").call("getblockcount") == 7 and len(calls) == 2
    finally:
        g.urllib.request.urlopen, g.time.sleep = saved, saved_sleep
    # sweep: PRESENT result dequeues
    cfg = json.loads(json.dumps(g.DEFAULTS))
    cfg.update({"network": "regtest", "fork_height": 1})
    cfg["btc"]["rpc_url"] = cfg["ecx"]["rpc_url"] = "http://127.0.0.1:1/"
    cfg["state"]["path"] = os.path.join(root, "s.sqlite")
    cfg["report"]["status_file"] = None
    cfg["run"]["dry_run"] = False
    br = g.Bridge(cfg)
    txid = "ab" * 32
    br.state.retry_add(txid, 5, 1, g.RETRY, "cluster", "00", [("cd" * 32, 0)])
    br.retry_kinds = br.state.retry_kinds()
    def fake_try_call(method, *params):
        if method == "getrawtransaction":
            return None, {"code": -5, "message": "No such mempool or blockchain transaction"}
        if method == "getmempoolinfo":
            return {"usage": 0, "maxmempool": 0}, None
        raise AssertionError(method)
    br.ecx.try_call = fake_try_call
    br.submit_one = lambda tx: (g.PRESENT, "mempool", "txn-already-in-mempool")
    br.sweep()
    assert br.state.retry_count() == 0 and br.state.pending_get(txid) is not None
    return "batch error object -> RPCError; IncompleteRead retried; sweep PRESENT dequeued + pending"


SCENARIOS = [
    ("T1", t01_plain), ("T2", t02_cluster), ("T3", t03_coinbase), ("T5", t05_rbf), ("T8", t08_csv),
    ("T10", t10_crash_and_concurrency), ("T12", t12_dry_run), ("T13", t13_confirmed_file),
    ("T6", t06_reorg), ("T7", t07_package), ("T14", t14_simulate), ("T15", t15_live_mode),
    ("T16", t16_evicted_parent), ("T17", t17_no_give_up_on_transient), ("T18", t18_ecx_node_checks),
    ("T19", t19_cpfp_dry_run), ("T20", t20_pacing_calls), ("T21", t21_btc_tip_below_cursor),
    ("T22", t22_ecx_reorg_walk_bounded), ("T23", t23_replacement_failed), ("T24", t24_units),
]


def main(argv):
    want = set(a.upper() for a in argv) or {n for n, _ in SCENARIOS}
    results, port = [], 29200
    for name, fn in SCENARIOS:
        if name not in want:
            continue
        t0 = time.time()
        try:
            msg = fn(port)
            results.append((name, "PASS", msg, time.time() - t0))
        except Exception:
            results.append((name, "FAIL", traceback.format_exc(), time.time() - t0))
        port += 10
        print(f"{name:4s} {results[-1][1]}  ({results[-1][3]:.0f}s)  {results[-1][2].strip().splitlines()[-1][:160]}", flush=True)
    print("\n== summary ==")
    for name, res, msg, dt in results:
        print(f"{name:4s} {res:4s} {dt:5.0f}s  {fn_doc(name)}")
        if res == "FAIL":
            print(msg)
    failed = [n for n, r, _, _ in results if r == "FAIL"]
    return 1 if failed else 0


def fn_doc(name):
    for n, fn in SCENARIOS:
        if n == name:
            return (fn.__doc__ or "").strip().splitlines()[0]
    return ""


# pytest entry points
def test_t01(): t01_plain(29300)
def test_t02(): t02_cluster(29310)
def test_t03(): t03_coinbase(29320)
def test_t05(): t05_rbf(29330)
def test_t08(): t08_csv(29340)
def test_t10(): t10_crash_and_concurrency(29350)
def test_t12(): t12_dry_run(29360)
def test_t13(): t13_confirmed_file(29370)
def test_t06(): t06_reorg(29380)
def test_t07(): t07_package(29390)
def test_t14(): t14_simulate(29400)
def test_t15(): t15_live_mode(29410)
def test_t16(): t16_evicted_parent(29420)
def test_t17(): t17_no_give_up_on_transient(29430)
def test_t18(): t18_ecx_node_checks(29440)
def test_t19(): t19_cpfp_dry_run(29450)
def test_t20(): t20_pacing_calls(29460)
def test_t21(): t21_btc_tip_below_cursor(29470)
def test_t22(): t22_ecx_reorg_walk_bounded(29480)
def test_t23(): t23_replacement_failed(29490)
def test_t24(): t24_units(29500)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
