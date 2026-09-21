"""
Regtest harness for garp_replay: a vanilla Bitcoin Core node and an eCash (alphanet-binary)
node on the same regtest genesis, fed to a common height, then forked.

Nothing here is used by garp_replay.py itself; it is test support only.
"""
import base64
import json
import os
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.request

VANILLA_BIN = os.environ.get("GARP_VANILLA_BIN", "/mnt/ssd/bitcoin-29.0-official/bin")
ECX_BIN = os.environ.get("GARP_ECX_BIN", "/mnt/ssd/alphanet/bin")
ROOT = os.environ.get("GARP_REGTEST_ROOT", "/mnt/ssd/drivechains/garp/.regtest")
RPC_USER, RPC_PASS = "garp", "garp"


class RPC:
    """Minimal JSON-RPC client. Raises RPCError on a JSON-RPC error."""

    def __init__(self, url, user=RPC_USER, password=RPC_PASS, timeout=120):
        self.url = url
        self.auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.timeout = timeout

    def __call__(self, method, *params):
        body = json.dumps({"jsonrpc": "1.0", "id": "t", "method": method, "params": list(params)}).encode()
        req = urllib.request.Request(self.url, body, {"Authorization": "Basic " + self.auth,
                                                      "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                d = json.load(r)
        except urllib.error.HTTPError as e:
            d = json.loads(e.read())
        if d.get("error"):
            raise RPCError(method, d["error"])
        return d["result"]


class RPCError(RuntimeError):
    def __init__(self, method, err):
        super().__init__(f"{method}: {err.get('code')} {err.get('message')}")
        self.code = err.get("code")
        self.message = err.get("message")


class Node:
    """One regtest bitcoind (vanilla or eCash), -listen=0 -connect=0, own datadir, wallet 'w'."""

    def __init__(self, name, bindir, datadir, rpcport, extra=()):
        self.name, self.bindir, self.datadir, self.rpcport = name, bindir, datadir, rpcport
        self.extra = list(extra)
        self.proc = None
        self.rpc = RPC(f"http://127.0.0.1:{rpcport}/")
        self.url = f"http://{RPC_USER}:{RPC_PASS}@127.0.0.1:{rpcport}/"

    def args(self):
        return [f"{self.bindir}/bitcoind", "-regtest", f"-datadir={self.datadir}", "-listen=0", "-connect=0",
                "-txindex=1", f"-rpcport={self.rpcport}", f"-rpcuser={RPC_USER}", f"-rpcpassword={RPC_PASS}",
                "-fallbackfee=0.0001", "-wallet=w", "-server=1", "-printtoconsole=0", "-dnsseed=0",
                "-listenonion=0"] + self.extra

    def start(self, wait=60):
        os.makedirs(self.datadir, exist_ok=True)
        self.proc = subprocess.Popen(self.args(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        t0 = time.time()
        while time.time() - t0 < wait:
            try:
                self.rpc("getblockchaininfo")
                return self
            except Exception:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"{self.name} exited with {self.proc.returncode}")
                time.sleep(0.25)
        raise RuntimeError(f"{self.name} did not come up")

    def stop(self):
        if self.proc is None:
            return
        try:
            self.rpc("stop")
        except Exception:
            pass
        try:
            self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.proc = None

    def kill(self):
        if self.proc is not None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
            self.proc = None

    def restart(self, extra=None):
        self.stop()
        if extra is not None:
            self.extra = list(extra)
        return self.start()

    # --- convenience -------------------------------------------------------------------------
    def height(self):
        return self.rpc("getblockcount")

    def tip(self):
        return self.rpc("getbestblockhash")

    def mine(self, n=1, addr=None):
        addr = addr or self.rpc("getnewaddress")
        return self.rpc("generatetoaddress", n, addr)

    def mempool(self):
        return set(self.rpc("getrawmempool"))

    def has_tx(self, txid):
        try:
            self.rpc("getrawtransaction", txid, 1)
            return True
        except RPCError:
            return False

    def confirmed(self, txid):
        try:
            return bool(self.rpc("getrawtransaction", txid, 1).get("blockhash"))
        except RPCError:
            return False


def feed(src, dst, until=None):
    """Relay src's blocks above dst's height (block_relay.py pattern): getblock 0 -> submitblock."""
    sh, dh = src.height(), dst.height()
    top = sh if until is None else min(sh, until)
    for h in range(dh + 1, top + 1):
        bh = src.rpc("getblockhash", h)
        assert dst.tip() == src.rpc("getblockheader", bh)["previousblockhash"], "dst tip is not the parent"
        res = dst.rpc("submitblock", src.rpc("getblock", bh, 0))
        assert res in (None, "duplicate"), f"submitblock {h}: {res}"
    assert src.rpc("getblockhash", top) == dst.rpc("getblockhash", top)


def import_wallet(src, dst):
    """Import src's descriptors (with private keys) into dst so both nodes control the same coins."""
    descs = src.rpc("listdescriptors", True)["descriptors"]
    req = []
    for d in descs:
        r = {"desc": d["desc"], "timestamp": 0, "active": d.get("active", False)}
        if d.get("active"):
            r["internal"] = d.get("internal", False)
        if "range" in d:
            r["range"] = [0, 999]
        req.append(r)
    res = dst.rpc("importdescriptors", req)
    assert all(x["success"] for x in res), res
    dst.rpc("rescanblockchain")


class Pair:
    """A forked (vanilla, ecx) regtest pair sharing the first `fork_height` blocks and one wallet."""

    def __init__(self, tag, ecx_extra=(), van_extra=(), portbase=29000):
        self.root = os.path.join(ROOT, tag)
        kill_stale(self.root)
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)
        self.van = Node("van", VANILLA_BIN, os.path.join(self.root, "van"), portbase, van_extra)
        self.ecx = Node("ecx", ECX_BIN, os.path.join(self.root, "ecx"), portbase + 1, ecx_extra)
        self.fork_height = None

    def start(self):
        self.van.start()
        self.ecx.start()
        return self

    def stop(self):
        self.van.stop()
        self.ecx.stop()

    def setup(self, shared_height=201, fanout=64, fanout_value=0.5, ecx_ahead=20):
        """Create the wallet, mine to `shared_height` on vanilla (with one fan-out tx so many small
        independent coins exist), copy the chain to ecx, import the wallet, then fork by mining
        `ecx_ahead` blocks on ecx only (ECX is far ahead of BTC in height on the real networks)."""
        self.van.rpc("createwallet", "w")
        self.ecx.rpc("createwallet", "w")
        addr = self.van.rpc("getnewaddress")
        self.van.mine(shared_height - 2, addr)  # leave room: one block with the fan-out, one after
        # fan-out: one mature coinbase -> `fanout` coins, mined into the shared chain
        outs = {self.van.rpc("getnewaddress"): fanout_value for _ in range(fanout)}
        raw = self.van.rpc("createrawtransaction", [], outs)
        funded = self.van.rpc("fundrawtransaction", raw)["hex"]
        signed = self.van.rpc("signrawtransactionwithwallet", funded)["hex"]
        self.van.rpc("sendrawtransaction", signed)
        self.van.mine(2, addr)
        assert self.van.height() == shared_height
        feed(self.van, self.ecx)
        import_wallet(self.van, self.ecx)
        assert self.van.rpc("getbalance") == self.ecx.rpc("getbalance")
        self.fork_height = shared_height + 1  # first height that is NOT shared
        if ecx_ahead:
            self.ecx.mine(ecx_ahead)
        return self

    # --- transaction builders (locktime 0 unless asked, so ECX height never matters) --------------
    def coins(self, node=None, min_amount=0, max_amount=None):
        node = node or self.van
        us = [u for u in node.rpc("listunspent", 1) if u["amount"] >= min_amount and u["spendable"]
              and (max_amount is None or u["amount"] <= max_amount)]
        us.sort(key=lambda u: (u["amount"], u["txid"], u["vout"]))
        return us

    def make_tx(self, inputs, outputs, fee=0.0001, node=None, locktime=0, sequence=None, version=None,
                parents=None):
        """inputs: [(txid, vout, amount)], outputs: {addr: amount} or a count of self-outputs.
        Returns (txid, hex). Fee = sum(in) - sum(out) is enforced by the caller's amounts unless
        `outputs` is an int, in which case the change is split evenly minus `fee`.
        `parents` ({txid: hex}) lets the wallet sign spends of txs it has not seen (not in its
        mempool or chain) by passing them as prevtxs."""
        node = node or self.van
        prevtxs = []
        for ptxid, phex in (parents or {}).items():
            dec = node.rpc("decoderawtransaction", phex)
            for txid, vout, _ in inputs:
                if txid == ptxid:
                    o = dec["vout"][vout]
                    prevtxs.append({"txid": ptxid, "vout": vout, "scriptPubKey": o["scriptPubKey"]["hex"],
                                    "amount": o["value"]})
        total_in = sum(a for _, _, a in inputs)
        if isinstance(outputs, int):
            each = round((total_in - fee) / outputs, 8)
            outputs = {node.rpc("getnewaddress"): each for _ in range(outputs)}
        vin = []
        for txid, vout, _ in inputs:
            e = {"txid": txid, "vout": vout}
            if sequence is not None:
                e["sequence"] = sequence
            vin.append(e)
        raw = node.rpc("createrawtransaction", vin, outputs, locktime)
        if version is not None:
            raw = set_version(raw, version)
        signed = node.rpc("signrawtransactionwithwallet", raw, prevtxs)
        assert signed["complete"], signed
        txid = node.rpc("decoderawtransaction", signed["hex"])["txid"]
        return txid, signed["hex"]

    def spend_coin(self, coin, fee=0.0001, node=None, **kw):
        """Spend one listunspent entry to a fresh address of the same wallet; returns (txid, hex, vout0_amount)."""
        node = node or self.van
        amt = round(float(coin["amount"]) - fee, 8)
        txid, hx = self.make_tx([(coin["txid"], coin["vout"], float(coin["amount"]))],
                                {node.rpc("getnewaddress"): amt}, node=node, **kw)
        return txid, hx, amt


def set_version(rawhex, version):
    """Rewrite the 4-byte little-endian nVersion of a raw transaction hex."""
    v = version.to_bytes(4, "little", signed=True).hex()
    return v + rawhex[8:]


def kill_stale(root):
    """SIGKILL any bitcoind still running on a datadir under `root` (left over from a crashed run)."""
    out = subprocess.run(["pgrep", "-f", f"bitcoind .*-datadir={root}/"], capture_output=True, text=True).stdout
    for pid in out.split():
        try:
            os.kill(int(pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
    if out.split():
        time.sleep(1)


def free_ports(base):
    """Two consecutive ports from base that are not currently bound (best effort)."""
    import socket
    p = base
    while True:
        ok = True
        for q in (p, p + 1):
            with socket.socket() as s:
                try:
                    s.bind(("127.0.0.1", q))
                except OSError:
                    ok = False
        if ok:
            return p
        p += 2
