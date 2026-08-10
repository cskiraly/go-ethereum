#!/usr/bin/env python3
# Copyright 2026 The go-ethereum Authors
# SPDX-License-Identifier: LGPL-3.0-or-later

"""
A/B-test the txtracker bouncing-protection layers on a live geth node.

Drives each runtime toggle (main, drop, reject, fee_gate) through an OFF
phase, captures /debug/metrics around each phase, and emits a markdown
report that compares each OFF phase to the nearest preceding ON control
(not a single up-front baseline) so time-of-day drift is contained.

At exit the script restores the toggles to whatever state they had when
it started — not a forced "all ON". An interrupted or crashed run
leaves an audit record in snapshots.json with per-phase status.

Requires:
  - geth binary on PATH (used as `geth attach <ws-url> --exec ...`).
  - The remote node started with --ws --ws.api txtracker (or namespace
    in --ws.api list) and --metrics --metrics.addr 0.0.0.0.
  - Python 3.8+; stdlib only.

Run:
  txtracker_bouncing_ab.py \\
      --ws ws://NODE:8546 \\
      --metrics http://NODE:6060/debug/metrics \\
      --dwell 600 \\
      --output-dir /tmp/txtracker-ab

At defaults: ~76 minutes (warmup 5m + baseline 10m + 4×(control 2m + OFF
10m + recover 2m)). Use --dwell 120 --warmup 60 --control-dwell 60
--recover 60 for an ~18-min smoke test.
"""

import argparse
import atexit
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DEFAULT_FLAGS = {"main": True, "drop": True, "reject": True, "fee_gate": True}
VALID_FLAG_NAMES = frozenset(DEFAULT_FLAGS.keys())

# Counter / gauge metrics captured per phase, with per-phase rate +
# delta. Histograms are handled via HISTOGRAM_KEYS below.
RELEVANT_KEYS = [
    # Fetcher announce funnel
    "eth/fetcher/transaction/announces/in",
    "eth/fetcher/transaction/announces/known",
    "eth/fetcher/transaction/announces/underpriced",
    "eth/fetcher/transaction/announces/bouncing",
    "eth/fetcher/transaction/announces/onchain",
    "eth/fetcher/transaction/announces/dos",
    # Fetcher request/reply
    "eth/fetcher/transaction/request/out",
    "eth/fetcher/transaction/request/done",
    "eth/fetcher/transaction/request/timeout",
    "eth/fetcher/transaction/replies/in",
    "eth/fetcher/transaction/replies/underpriced",
    "eth/fetcher/transaction/replies/otherreject",
    "eth/fetcher/transaction/broadcasts/in",
    "eth/fetcher/transaction/broadcasts/underpriced",
    # Bouncing tracker (real)
    "eth/txtracker/bouncing/cleared",
    "eth/txtracker/bouncing/gate_blocked",
    "eth/txtracker/bouncing/inserted/drop",
    "eth/txtracker/bouncing/inserted/reject",
    "eth/txtracker/bouncing/size",
    # Bouncing tracker (whatif)
    "eth/txtracker/bouncing/whatif/suppressed",
    "eth/txtracker/bouncing/whatif/inserted_drop",
    "eth/txtracker/bouncing/whatif/inserted_reject",
    "eth/txtracker/bouncing/whatif/gate_blocked",
    # Pool cross-checks
    "txpool/underpriced",
    "txpool/valid",
    "txpool/known",
    "txpool/invalid",
    "txpool/pending",
    "txpool/queued",
    "txpool/pending/ratelimit",
    "txpool/queued/ratelimit",
    "txpool/pending/replace",
    # Direct bandwidth measurement (replaces the 300 B/body estimate
    # if these are exposed by the node's metrics registry).
    "p2p/ingress",
    "p2p/egress",
    "p2p/peers",
    # Chain context (gauge / counter — confirms test ran across blocks)
    "chain/head/header",
    "chain/inserts",
]

# Histogram metrics: stored as snapshot percentiles + mean, end-of-phase
# only (cannot be subtracted across phases). Used to compare
# distributions during ON vs OFF.
HISTOGRAM_KEYS = [
    "txpool/reorgtime",
    "txpool/reheap",
    "txpool/dropbetweenreorg",
]
HISTOGRAM_FIELDS = ["50-percentile", "95-percentile", "99-percentile", "mean"]


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def now_epoch():
    return time.time()


def log(msg):
    print(f"[{now_iso()}] {msg}", flush=True)


# -----------------------------------------------------------------------
# RPC via `geth attach`
# -----------------------------------------------------------------------

# Geth's console only auto-binds well-known namespaces (eth, admin,
# web3, net, ...). The txtracker namespace is exposed by the running
# node but its JS object is not pre-defined in the console. Prepend
# this snippet to every --exec to register the bouncing-flag methods
# on web3.txtracker before invoking them. Each `geth attach --exec`
# is a fresh process, so the registration must repeat per call.
TXTRACKER_EXTEND = (
    'web3._extend({property: "txtracker", methods: ['
    'new web3._extend.Method({name: "bouncingFlags", call: "txtracker_bouncingFlags", params: 0}),'
    'new web3._extend.Method({name: "setBouncingFlag", call: "txtracker_setBouncingFlag", params: 2})'
    ']});'
)


def rpc(geth_bin, ws_url, expr, attempts=2):
    """Evaluate a JS expression on the remote node, return parsed result.
    Retries once on transient failure."""
    last_err = None
    full_expr = TXTRACKER_EXTEND + expr
    for attempt in range(1, attempts + 1):
        proc = subprocess.run(
            [geth_bin, "attach", "--exec", full_expr, ws_url],
            capture_output=True, text=True, timeout=30,
        )
        if proc.returncode == 0:
            return parse_js_value(proc.stdout.strip())
        last_err = (
            f"geth attach failed (code {proc.returncode}) for {expr!r}: "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
        time.sleep(attempt)
    raise RuntimeError(last_err)


def parse_js_value(text):
    """Parse the raw output of `geth attach --exec`. Handles plain JSON
    numbers/booleans/strings, JS-style {key: val} objects (with optional
    string values single- or double-quoted), and `undefined` / `null`."""
    if text == "" or text == "undefined" or text == "null":
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # JS-style: {key: val, key: 'val'}. Quote unquoted keys; convert
    # single-quoted strings to double-quoted.
    fixed = re.sub(r'([{\s,])([A-Za-z_][A-Za-z0-9_]*):', r'\1"\2":', text)
    fixed = re.sub(r"'([^']*)'", r'"\1"', fixed)
    try:
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"could not parse geth output {text!r}: {e}")


def get_flags(geth_bin, ws_url):
    return rpc(geth_bin, ws_url, "web3.txtracker.bouncingFlags()")


def set_flag(geth_bin, ws_url, name, enabled):
    if name not in VALID_FLAG_NAMES:
        raise ValueError(f"refusing to set unknown flag {name!r}; valid: {sorted(VALID_FLAG_NAMES)}")
    val = "true" if enabled else "false"
    return rpc(geth_bin, ws_url, f'web3.txtracker.setBouncingFlag("{name}", {val})')


def apply_flags(geth_bin, ws_url, target):
    """Bring the toggle state to `target`; flip only flags that differ
    and verify post-apply the node reports what we asked for. Raises if
    the read-back doesn't match (e.g. another operator is racing us)."""
    cur = get_flags(geth_bin, ws_url) or {}
    for name, want in target.items():
        if cur.get(name) != want:
            set_flag(geth_bin, ws_url, name, want)
    after = get_flags(geth_bin, ws_url) or {}
    mismatch = {k: (after.get(k), v) for k, v in target.items() if after.get(k) != v}
    if mismatch:
        raise RuntimeError(f"flags didn't take effect; mismatch (got, want): {mismatch}")


def restore_flags(geth_bin, ws_url, target, attempts=3):
    """Idempotent restore — used at exit. Tries up to `attempts` times
    with linear backoff. If we fail after retries, log a loud warning
    rather than raising, since this fires from atexit."""
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            apply_flags(geth_bin, ws_url, target)
            log(f"restored toggles to {target}")
            return
        except Exception as e:
            last_err = e
            time.sleep(attempt)
    log(f"WARNING: failed to restore toggles after {attempts} attempts: {last_err}")
    log(f"WARNING: node may be left with non-default toggle state; please verify manually")


# -----------------------------------------------------------------------
# Metrics scraping
# -----------------------------------------------------------------------

def scrape(metrics_url, attempts=3):
    """Fetch /debug/metrics and flatten any nested structure into a
    dict of dotted keys -> numeric values. Retries on URL/JSON errors
    with linear backoff so a transient blip doesn't kill a long phase."""
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(metrics_url, timeout=20) as resp:
                raw = json.loads(resp.read())
            break
        except (urllib.error.URLError, json.JSONDecodeError, OSError, TimeoutError) as e:
            last_err = e
            if attempt == attempts:
                raise RuntimeError(f"scrape {metrics_url!r} failed after {attempts} attempts: {e}") from e
            time.sleep(attempt)
    flat = {}

    def walk(prefix, node):
        if isinstance(node, dict):
            for k, v in node.items():
                child = f"{prefix}.{k}" if prefix else k
                walk(child, v)
        elif isinstance(node, list):
            # Stringify so the value at least round-trips through JSON;
            # we don't analyse list-shaped metrics today.
            flat[prefix] = json.dumps(node)
        else:
            flat[prefix] = node

    walk("", raw)
    return flat


def metric_get(snap, key, suffix=".count"):
    """Look up snap[key + suffix], or snap[key] if key is already a
    full leaf. Returns 0 if not found."""
    if key + suffix in snap:
        return snap[key + suffix]
    if key in snap:
        return snap[key]
    return 0


# -----------------------------------------------------------------------
# Phase runner
# -----------------------------------------------------------------------

class PhaseError(RuntimeError):
    """Raised when a phase fails after producing partial state. Carries
    the partial record so the caller can persist it."""
    def __init__(self, record, cause):
        super().__init__(str(cause))
        self.record = record


def run_phase(geth_bin, ws_url, metrics_url, label, flags, dwell, capture):
    """Apply `flags`, sleep `dwell` seconds, capture metrics. Returns
    a phase dict. On failure, raises PhaseError with a partial record
    so the caller persists what we have before exiting."""
    log(f"phase {label}: setting flags {flags}")
    record = {
        "label": label,
        "flags": dict(flags),
        "dwell_target": dwell,
        "start": now_iso(),
        "start_epoch": now_epoch(),
        "status": "running",
    }
    try:
        apply_flags(geth_bin, ws_url, flags)

        if capture:
            log(f"phase {label}: capturing metrics at start")
            record["snapshot_start"] = scrape(metrics_url)
            record["scrape_start_epoch"] = now_epoch()

        log(f"phase {label}: dwell {dwell}s")
        time.sleep(dwell)

        if capture:
            log(f"phase {label}: capturing metrics at end")
            record["snapshot_end"] = scrape(metrics_url)
            record["scrape_end_epoch"] = now_epoch()
            record["dwell_actual"] = (
                record["scrape_end_epoch"] - record["scrape_start_epoch"]
            )
            record["deltas"] = compute_deltas(record["snapshot_start"], record["snapshot_end"])
            record["histograms_end"] = capture_histograms(record["snapshot_end"])

        record["end"] = now_iso()
        record["status"] = "ok"
        return record
    except Exception as e:
        record["end"] = now_iso()
        record["status"] = "failed"
        record["error"] = f"{type(e).__name__}: {e}"
        raise PhaseError(record, e) from e


def compute_deltas(start, end):
    """Per-key diffs for RELEVANT_KEYS. Counter keys diff `.count`;
    keys without `.count` (gauges) report start/end/delta."""
    deltas = {}
    for key in RELEVANT_KEYS:
        sc = metric_get(start, key, ".count")
        ec = metric_get(end, key, ".count")
        if isinstance(sc, (int, float)) and isinstance(ec, (int, float)) and (sc or ec):
            deltas[key + ".count"] = ec - sc
            continue
        sv = metric_get(start, key, "")
        ev = metric_get(end, key, "")
        if isinstance(sv, (int, float)) and isinstance(ev, (int, float)):
            deltas[key] = {"start": sv, "end": ev, "delta": ev - sv}
    return deltas


def capture_histograms(snap):
    """Snapshot percentile + mean for each histogram metric. Stored
    end-of-phase only — they're distributions, not counters."""
    out = {}
    for key in HISTOGRAM_KEYS:
        per_field = {}
        for field in HISTOGRAM_FIELDS:
            full = f"{key}.{field}"
            if full in snap:
                per_field[field] = snap[full]
        if per_field:
            out[key] = per_field
    return out


# -----------------------------------------------------------------------
# Analysis / report
# -----------------------------------------------------------------------

def rate(p, key):
    """Per-second rate for a counter delta in phase p. Uses actual
    scrape-to-scrape elapsed when available, falling back to dwell_target."""
    if "deltas" not in p:
        return 0.0
    d = p["deltas"].get(key, 0)
    if isinstance(d, dict):
        d = d.get("delta", 0)
    elapsed = p.get("dwell_actual") or p.get("dwell_target") or 1
    return d / elapsed if elapsed else 0.0


def total(p, key):
    """Raw delta count for a counter, or 0."""
    if "deltas" not in p:
        return 0
    d = p["deltas"].get(key, 0)
    if isinstance(d, dict):
        d = d.get("delta", 0)
    return d


def find_nearest_control(phases, idx):
    """Walk backwards from index `idx` looking for the most recent
    captured ON-state phase. Used as the local control for OFF
    comparison so 30+ minutes of mainnet drift doesn't pollute the diff."""
    for i in range(idx - 1, -1, -1):
        p = phases[i]
        if p.get("status") != "ok" or "deltas" not in p:
            continue
        if all(p["flags"].get(k, True) for k in DEFAULT_FLAGS):
            return p
    return None


def fmt_qty(x):
    return f"{x:,.0f}" if abs(x) >= 1 else f"{x:.3f}"


def analyze(phases):
    """Build the markdown report. Each OFF phase is compared to its
    nearest preceding ON control (not the global baseline)."""
    measured = [p for p in phases if p.get("status") == "ok" and "deltas" in p]
    by_idx = {id(p): i for i, p in enumerate(phases)}

    lines = []
    lines.append("# Bouncing-protection A/B report")
    lines.append("")
    lines.append(f"Generated: {now_iso()}")
    lines.append("")
    if any(p.get("status") == "failed" for p in phases):
        lines.append("> **WARNING:** at least one phase failed. See `snapshots.json` for the audit trail.")
        lines.append("")

    # Phase summary table.
    lines.append("## Phase summary")
    lines.append("")
    lines.append("| Phase | Flags | Dwell (actual) | bouncing/s | whatif/sup /s | request/out /s | replies/under /s |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for p in measured:
        flags_str = ",".join(f"{k}={'1' if v else '0'}" for k, v in p["flags"].items())
        elapsed = p.get("dwell_actual") or p.get("dwell_target") or 0
        lines.append(
            f"| `{p['label']}` "
            f"| `{flags_str}` "
            f"| {elapsed:.0f}s "
            f"| {rate(p, 'eth/fetcher/transaction/announces/bouncing.count'):.2f} "
            f"| {rate(p, 'eth/txtracker/bouncing/whatif/suppressed.count'):.2f} "
            f"| {rate(p, 'eth/fetcher/transaction/request/out.count'):.2f} "
            f"| {rate(p, 'eth/fetcher/transaction/replies/underpriced.count'):.2f} |"
        )
    lines.append("")

    # Per-toggle comparisons against nearest prior ON control.
    lines.append("## Per-toggle vs nearest control")
    lines.append("")

    plan_for = {
        "main_off": [
            ("eth/fetcher/transaction/announces/bouncing.count", "freeze"),
            ("eth/txtracker/bouncing/whatif/suppressed.count", "up"),
            ("eth/fetcher/transaction/request/out.count", "up"),
            ("eth/fetcher/transaction/replies/in.count", "up"),
            ("eth/fetcher/transaction/replies/underpriced.count", "up"),
            ("p2p/ingress.count", "up"),
        ],
        "drop_off": [
            ("eth/txtracker/bouncing/inserted/drop.count", "freeze"),
            ("eth/txtracker/bouncing/whatif/inserted_drop.count", "up"),
            ("eth/txtracker/bouncing/inserted/reject.count", "flat"),
            ("eth/fetcher/transaction/announces/bouncing.count", "down"),
        ],
        "reject_off": [
            ("eth/txtracker/bouncing/inserted/reject.count", "freeze"),
            ("eth/txtracker/bouncing/whatif/inserted_reject.count", "up"),
            ("eth/txtracker/bouncing/inserted/drop.count", "flat"),
            ("eth/fetcher/transaction/announces/bouncing.count", "down"),
        ],
        "fee_gate_off": [
            ("eth/txtracker/bouncing/gate_blocked.count", "freeze"),
            ("eth/txtracker/bouncing/whatif/gate_blocked.count", "up"),
            ("eth/txtracker/bouncing/cleared.count", "up"),
        ],
    }

    for p in measured:
        if p["label"] not in plan_for:
            continue
        idx_in_phases = next((i for i, ph in enumerate(phases) if ph is p), None)
        ctrl = find_nearest_control(phases, idx_in_phases) if idx_in_phases is not None else None
        if ctrl is None:
            lines.append(f"### `{p['label']}` (no control found, skipping)")
            lines.append("")
            continue
        lines.append(f"### `{p['label']}` vs `{ctrl['label']}`")
        lines.append("")
        lines.append("| Metric | Control /s | Phase /s | Δ /s | Phase total | Expected | OK? |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for key, expected in plan_for[p["label"]]:
            cr = rate(ctrl, key)
            pr = rate(p, key)
            delta = pr - cr
            pt = total(p, key)
            ok = check_expected(expected, cr, pr, delta)
            lines.append(
                f"| `{key}` | {cr:.2f} | {pr:.2f} | {delta:+.2f} "
                f"| {fmt_qty(pt)} | {expected} | {'✅' if ok else '❌'} |"
            )
        lines.append("")

    # Sanity: peer count drift across the whole run.
    if measured:
        first = measured[0]
        last = measured[-1]
        first_peers = first["deltas"].get("p2p/peers", {}).get("start") if isinstance(first["deltas"].get("p2p/peers"), dict) else None
        last_peers = last["deltas"].get("p2p/peers", {}).get("end") if isinstance(last["deltas"].get("p2p/peers"), dict) else None
        if first_peers is not None and last_peers is not None:
            drift = last_peers - first_peers
            pct = 100 * drift / first_peers if first_peers else 0
            lines.append("## Sanity")
            lines.append("")
            lines.append(f"- peer count: {first_peers:.0f} → {last_peers:.0f} (Δ {drift:+.0f}, {pct:+.1f}%)")
            if abs(pct) > 10:
                lines.append("  > **WARNING:** >10% peer churn during the run; comparisons may be biased.")
            lines.append("")

    # Headline savings: aggregate over baseline phase if present.
    baseline = next((p for p in measured if p["label"] == "baseline"), None)
    if baseline and baseline["deltas"].get("eth/fetcher/transaction/announces/in.count", 0) > 0:
        lines.append("## Baseline savings estimate")
        lines.append("")
        b_in = total(baseline, "eth/fetcher/transaction/announces/in.count")
        b_known = total(baseline, "eth/fetcher/transaction/announces/known.count")
        b_under = total(baseline, "eth/fetcher/transaction/announces/underpriced.count")
        b_bounce = total(baseline, "eth/fetcher/transaction/announces/bouncing.count")
        b_onchain = total(baseline, "eth/fetcher/transaction/announces/onchain.count")
        b_dos = total(baseline, "eth/fetcher/transaction/announces/dos.count")
        b_req = total(baseline, "eth/fetcher/transaction/request/out.count")
        elapsed = baseline.get("dwell_actual") or baseline.get("dwell_target") or 1
        would_fetch = max(b_in - b_known - b_under - b_bounce - b_onchain - b_dos, 0)
        ratio = b_req / would_fetch if would_fetch else 0
        saved_fetches = b_bounce * ratio if ratio else 0

        lines.append(f"- announces/in:        {b_in:,}")
        lines.append(f"- announces/bouncing:  {b_bounce:,}" + (f" ({100*b_bounce/b_in:.2f}% of in)" if b_in else ""))
        lines.append(f"- request/out:         {b_req:,}")
        lines.append(f"- would-be-fetched:    {would_fetch:,}")
        lines.append(f"- announce→fetch conversion ratio: {100*ratio:.1f}%")
        lines.append(f"- estimated saved fetches (over {elapsed:.0f}s): {saved_fetches:,.0f}")

        # Direct bandwidth measurement, if p2p/ingress is exposed.
        ingress_during_baseline = total(baseline, "p2p/ingress.count")
        # Cross-check with main_off if present.
        main_off = next((p for p in measured if p["label"] == "main_off"), None)
        if main_off:
            ctrl = find_nearest_control(phases, next((i for i, ph in enumerate(phases) if ph is main_off), 0))
            if ctrl:
                ctrl_ingress_rate = rate(ctrl, "p2p/ingress.count")
                off_ingress_rate = rate(main_off, "p2p/ingress.count")
                if ctrl_ingress_rate or off_ingress_rate:
                    diff_per_sec = off_ingress_rate - ctrl_ingress_rate
                    lines.append("")
                    lines.append("### Direct bandwidth measurement (p2p/ingress)")
                    lines.append("")
                    lines.append(f"- control phase rate: {ctrl_ingress_rate/1e6:.3f} MB/s")
                    lines.append(f"- main_off rate:      {off_ingress_rate/1e6:.3f} MB/s")
                    lines.append(f"- delta:              {diff_per_sec/1e6:+.3f} MB/s ({diff_per_sec*3600/1e9:+.2f} GB/hour)")
                    if diff_per_sec > 0:
                        gb_month = diff_per_sec * 86400 * 30 / 1e9
                        lines.append(f"- extrapolated savings: {gb_month:.2f} GB/month inbound P2P")
        else:
            # No direct measurement — fall back to estimate.
            mb_saved = saved_fetches * 300 / 1e6
            mb_per_hour = mb_saved * 3600 / elapsed if elapsed else 0
            lines.append(f"- estimated inbound saved (300 B/body): ~{mb_saved:.1f} MB"
                         f" ({mb_per_hour:.1f} MB/h ≈ {mb_per_hour*24*30/1024:.1f} GB/month)")

        b_cleared = total(baseline, "eth/txtracker/bouncing/cleared.count")
        b_blocked = total(baseline, "eth/txtracker/bouncing/gate_blocked.count")
        if b_cleared + b_blocked:
            lines.append(f"- fee-gate save rate:  {100*b_blocked/(b_cleared+b_blocked):.1f}%"
                         f" ({b_blocked:,} / {b_cleared+b_blocked:,})")

        b_drop = total(baseline, "eth/txtracker/bouncing/inserted/drop.count")
        b_rej = total(baseline, "eth/txtracker/bouncing/inserted/reject.count")
        if b_drop + b_rej:
            lines.append(f"- insert split: drop={b_drop:,}"
                         f" ({100*b_drop/(b_drop+b_rej):.1f}%) /"
                         f" reject={b_rej:,} ({100*b_rej/(b_drop+b_rej):.1f}%)")
        lines.append("")

    # Histogram comparison: reorgtime, reheap distributions.
    lines.append("## Pool latency distributions (end-of-phase)")
    lines.append("")
    lines.append("Histograms can't be deltaed across phases; reported as snapshot percentiles taken at the end of each measured phase.")
    lines.append("")
    for hkey in HISTOGRAM_KEYS:
        rows = []
        for p in measured:
            h = p.get("histograms_end", {}).get(hkey)
            if h:
                rows.append((p["label"], h))
        if not rows:
            continue
        lines.append(f"### `{hkey}`")
        lines.append("")
        header = "| Phase | " + " | ".join(HISTOGRAM_FIELDS) + " |"
        sep = "| --- " * (len(HISTOGRAM_FIELDS) + 1) + "|"
        lines.append(header)
        lines.append(sep)
        for label, h in rows:
            cells = " | ".join(f"{h.get(f, 0):.1f}" if isinstance(h.get(f), (int, float)) else "—" for f in HISTOGRAM_FIELDS)
            lines.append(f"| `{label}` | {cells} |")
        lines.append("")

    return "\n".join(lines) + "\n"


def check_expected(expected, ctrl_rate, phase_rate, delta_rate):
    """Volume-aware tolerance check. Floor at 0.05/s OR 5% of control,
    whichever larger; for 'up'/'down' require absolute delta > floor."""
    floor = max(0.05, 0.05 * abs(ctrl_rate))
    if expected == "freeze":
        # Off-state should produce ~0; allow drift below floor.
        return abs(phase_rate) <= floor
    if expected == "flat":
        return abs(delta_rate) <= floor
    if expected == "up":
        return delta_rate > floor
    if expected == "down":
        return delta_rate < -floor
    return False


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def build_plan(args):
    """Compose the phase plan. Each toggle-OFF phase is preceded by a
    short ON control (so the comparison uses adjacent traffic, not a
    single up-front baseline) and followed by a recovery phase."""
    toggle_phases = [
        ("main_off",     {**DEFAULT_FLAGS, "main": False}),
        ("drop_off",     {**DEFAULT_FLAGS, "drop": False}),
        ("reject_off",   {**DEFAULT_FLAGS, "reject": False}),
        ("fee_gate_off", {**DEFAULT_FLAGS, "fee_gate": False}),
    ]
    if args.shuffle_toggles:
        random.shuffle(toggle_phases)
        log(f"shuffled toggle order: {[t[0] for t in toggle_phases]}")

    plan = []
    if not args.no_warmup:
        plan.append(("warmup", DEFAULT_FLAGS, args.warmup, False))
    plan.append(("baseline", DEFAULT_FLAGS, args.dwell, True))
    for label, flags_for_phase in toggle_phases:
        plan.append((f"control_before_{label}", DEFAULT_FLAGS, args.control_dwell, True))
        plan.append((label, flags_for_phase, args.dwell, True))
        plan.append((f"recover_after_{label}", DEFAULT_FLAGS, args.recover, False))
    return plan


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ws", required=True, help="WebSocket RPC URL of geth (e.g. ws://node:8546)")
    ap.add_argument("--metrics", required=True, help="HTTP /debug/metrics URL (e.g. http://node:6060/debug/metrics)")
    ap.add_argument("--geth", default="geth", help="Path to geth binary (default: geth on PATH)")
    ap.add_argument("--dwell", type=int, default=600, help="Per-OFF-phase dwell, seconds (default 600)")
    ap.add_argument("--warmup", type=int, default=300, help="Initial warmup, no measurement (default 300)")
    ap.add_argument("--control-dwell", type=int, default=120, help="ON-control phase dwell before each OFF (default 120)")
    ap.add_argument("--recover", type=int, default=120, help="Recovery time after each OFF phase (default 120)")
    ap.add_argument("--output-dir", default=None, help="Where to write snapshots.json + report.md (default: ./txtracker-ab-<UTC-timestamp>)")
    ap.add_argument("--no-warmup", action="store_true", help="Skip the warmup phase")
    ap.add_argument("--shuffle-toggles", action="store_true", help="Randomize toggle-off order to reduce time-of-day bias")
    args = ap.parse_args()

    if args.output_dir is None:
        # Stamp each run with a UTC timestamp so successive runs don't
        # overwrite each other's snapshots.json / report.md. ISO-ish
        # but filesystem-friendly (no colons).
        args.output_dir = "./txtracker-ab-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if shutil.which(args.geth) is None and not os.path.isfile(args.geth):
        print(f"error: geth binary {args.geth!r} not found on PATH or as a file", file=sys.stderr)
        sys.exit(2)

    # Sanity: can we reach RPC + metrics?
    log(f"connecting to {args.ws}")
    original_flags = get_flags(args.geth, args.ws)
    if not isinstance(original_flags, dict) or set(original_flags.keys()) != VALID_FLAG_NAMES:
        print(f"error: unexpected bouncingFlags() result {original_flags!r}; node may not be on the txtracker-bouncing-toggles branch", file=sys.stderr)
        sys.exit(2)
    log(f"current (original) flags: {original_flags}")

    log(f"scraping {args.metrics}")
    snap = scrape(args.metrics)
    if "eth/txtracker/bouncing/cleared.count" not in snap:
        log("WARNING: bouncing metrics not present in /debug/metrics — node may not be running this branch")

    os.makedirs(args.output_dir, exist_ok=True)

    # Restore the operator's original flag state at exit, no matter how
    # we leave. atexit + signal handlers cover Ctrl+C and SIGTERM but
    # not SIGKILL or hard crashes — verify manually if the process
    # disappears unexpectedly.
    atexit.register(restore_flags, args.geth, args.ws, dict(original_flags))
    def on_signal(signum, frame):
        log(f"signal {signum} received, exiting")
        sys.exit(130)
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    plan = build_plan(args)
    total_seconds = sum(p[2] for p in plan)
    log(f"plan: {len(plan)} phases, total {total_seconds}s ({total_seconds/60:.1f} min)")

    phases = []
    snapshots_path = os.path.join(args.output_dir, "snapshots.json")
    report_path = os.path.join(args.output_dir, "report.md")

    def persist(extra_meta=None):
        payload = {"phases": phases}
        if extra_meta:
            payload.update(extra_meta)
        with open(snapshots_path, "w") as f:
            json.dump(payload, f, indent=2)

    persist({"started": now_iso(), "args": vars(args), "original_flags": original_flags})

    aborted = False
    try:
        for label, flags_for_phase, dwell, capture in plan:
            try:
                ph = run_phase(args.geth, args.ws, args.metrics, label, flags_for_phase, dwell, capture)
                phases.append(ph)
            except PhaseError as e:
                phases.append(e.record)
                log(f"phase {label} failed: {e}; aborting plan")
                aborted = True
                break
            persist({"started": now_iso(), "args": vars(args), "original_flags": original_flags})
    finally:
        persist({"started": now_iso(), "args": vars(args), "original_flags": original_flags, "aborted": aborted})

    report = analyze(phases)
    with open(report_path, "w") as f:
        f.write(report)
    log(f"wrote {snapshots_path}")
    log(f"wrote {report_path}")
    print()
    print(report)
    if aborted:
        sys.exit(1)


if __name__ == "__main__":
    main()
