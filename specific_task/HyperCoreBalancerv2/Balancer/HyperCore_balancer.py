#!/usr/bin/env python3

import os
import sys
import gc
import time
import signal
import random
import threading
import warnings
import traceback
import requests
from collections import deque
from statistics import mean
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Load environment variables
load_dotenv()

import config_db

# =============================================================================
# Configuration & Defaults
# =============================================================================

def get_config(key: str, default: Any, cast_type: type = str) -> Any:
    """Helper to fetch and cast environment variables (used for configurables only)."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        if cast_type == bool:
            return value.lower() in ('true', '1', 'yes', 'y')
        if cast_type == list:
            return [ip.strip() for ip in value.split(',') if ip.strip()]
        return cast_type(value)
    except (ValueError, TypeError):
        return default

# Cluster Credentials & Endpoint (configurables — stay in .env)
SC_HOST = get_config('SC_HOST', "https://cluster-ip/rest/v1")
SC_USERNAME = get_config('SC_USERNAME', "admin")
SC_PASSWORD = get_config('SC_PASSWORD', "password")
SC_VERIFY_SSL = get_config('SC_VERIFY_SSL', False, bool)

# InfluxDB (for writing migration events — configurables)
INFLUX_URL = get_config('INFLUX_URL', "http://influxdb:8086")
INFLUX_TOKEN = get_config('INFLUX_TOKEN', "super-secret-auth-token")
INFLUX_ORG = get_config('INFLUX_ORG', "hypercore")
INFLUX_BUCKET = get_config('INFLUX_BUCKET', "metrics")

# Tunables are loaded from config_db at startup (seeded from env vars on first run)

# =============================================================================
# API Client (with automatic re-authentication on 401)
# =============================================================================

class HyperCoreApiClient:
    def __init__(self, base_url: str, user: str, pw: str, verify: bool):
        self.base_url = base_url.rstrip('/')
        self.user = user
        self.pw = pw
        self.session = requests.Session()
        self.session.verify = verify
        
        if not verify:
            from urllib3.exceptions import InsecureRequestWarning
            warnings.simplefilter('ignore', InsecureRequestWarning)

    def login(self) -> bool:
        url = f"{self.base_url}/login"
        payload = {"username": self.user, "password": self.pw}
        try:
            self.session.cookies.clear()
            response = self.session.post(url, json=payload, timeout=10)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Login failed: {e}")
            return False

    def close(self):
        try:
            self.session.post(f"{self.base_url}/logout", timeout=5)
        except:
            pass
        self.session.close()

    def fetch(self, endpoint: str) -> Any:
        response = self.session.get(f"{self.base_url}{endpoint}", timeout=15)
        if response.status_code == 401:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Session expired (401). Re-authenticating...")
            if self.login():
                response = self.session.get(f"{self.base_url}{endpoint}", timeout=15)
            else:
                raise requests.exceptions.HTTPError("Re-authentication failed after 401")
        response.raise_for_status()
        return response.json()

    def get_nodes(self): return self.fetch("/Node")
    def get_vms(self): return self.fetch("/VirDomain")
    def get_vm_stats(self): return self.fetch("/VirDomainStats")
    
    def get_task_status(self, task_tag: str) -> str:
        try:
            res = self.fetch(f"/TaskTag/{task_tag}")
            return res[0]['state'] if res else "UNKNOWN"
        except:
            return "UNKNOWN"

    def migrate(self, vm_uuid: str, target_node: str) -> Dict:
        action = [{
            "virDomainUUID": vm_uuid, 
            "actionType": "LIVEMIGRATE", 
            "nodeUUID": target_node
        }]
        resp = self.session.post(f"{self.base_url}/VirDomain/action", json=action, timeout=15)
        if resp.status_code == 401:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Session expired (401) during migration. Re-authenticating...")
            if self.login():
                resp = self.session.post(f"{self.base_url}/VirDomain/action", json=action, timeout=15)
            else:
                raise requests.exceptions.HTTPError("Re-authentication failed during migration")
        resp.raise_for_status()
        return resp.json()

# =============================================================================
# Migration Event Logger (writes structured events to InfluxDB)
# =============================================================================

class MigrationLogger:
    """Writes migration events to InfluxDB so the dashboard can display them.
    
    Measurement: migration_events
    Tags:    engine, mode, dry_run, vm_name, source_ip, target_ip
    Fields:  vm_uuid, reason
    """
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.bucket = bucket
        try:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.available = True
        except Exception as e:
            print(f"[MIGRATION LOGGER] Could not connect to InfluxDB: {e}")
            self.available = False
    
    def log_event(self, vm_name: str, vm_uuid: str, source_ip: str, target_ip: str,
                  engine: str, mode: str, dry_run: bool, reason: str = ""):
        if not self.available:
            return
        try:
            point = Point("migration_events") \
                .tag("engine", engine) \
                .tag("mode", mode) \
                .tag("dry_run", str(dry_run).lower()) \
                .tag("vm_name", vm_name) \
                .tag("source_ip", source_ip) \
                .tag("target_ip", target_ip) \
                .field("vm_uuid", vm_uuid) \
                .field("reason", reason)
            self.write_api.write(bucket=self.bucket, record=point)
        except Exception as e:
            print(f"[MIGRATION LOGGER] Failed to write event: {e}")
    
    def close(self):
        if self.available:
            try:
                self.client.close()
            except:
                pass

# =============================================================================
# Balancer Engine
# =============================================================================

class LoadBalancer:
    def __init__(self, client: HyperCoreApiClient, config: Dict, migration_logger: MigrationLogger):
        self.client = client
        self.config = config
        self.migration_logger = migration_logger
        self.max_history = int((config['AVG_WINDOW_MINUTES'] * 60) / config['SAMPLE_INTERVAL_SECONDS'])

        self.node_cpu_history = {}
        self.node_ram_history = {}
        self.vm_cpu_history = {}

        self.vm_cooldowns = {}
        self.last_migration_time = 0
        self.active_task = None

        self.last_predictive_run = 0
        self.reserved_nodes = {}

        self._predictive_thread = None
        self._predictive_results = None
        self._predictive_overrun_logged = False
        self._pending_predictive_recs = []

        self._last_config_check = 0

    def reload_config(self):
        """Hot-reload tunables from SQLite. Logs any changed values."""
        new = config_db.get_all()
        for key in list(self.config.keys()):
            if key in new and new[key] != self.config[key]:
                self.log(1, f"[CONFIG] {key}: {self.config[key]} → {new[key]}")
                self.config[key] = new[key]
        # Recalculate derived window size; clear histories if it changed
        new_max = int((self.config['AVG_WINDOW_MINUTES'] * 60) / self.config['SAMPLE_INTERVAL_SECONDS'])
        if new_max != self.max_history:
            self.max_history = new_max
            self.node_cpu_history.clear()
            self.node_ram_history.clear()
            self.vm_cpu_history.clear()
            self.log(0, f"[CONFIG] History window resized to {self.max_history} samples — buffers cleared.")

    def log(self, level: int, message: str):
        if level == 0 or self.config['VERBOSITY'] >= level:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] {message}")

    def update_metrics(self, nodes, vm_stats):
        for n in nodes:
            uid = n['uuid']
            if uid not in self.node_cpu_history:
                self.node_cpu_history[uid] = deque(maxlen=self.max_history)
                self.node_ram_history[uid] = deque(maxlen=self.max_history)
            self.node_cpu_history[uid].append(n.get('cpuUsage', 0.0))
            mem_pct = (n.get('totalMemUsageBytes', 0) / n.get('memSize', 1)) * 100
            self.node_ram_history[uid].append(mem_pct)
            
        for s in vm_stats:
            uid = s['uuid']
            if uid not in self.vm_cpu_history:
                self.vm_cpu_history[uid] = deque(maxlen=self.max_history)
            self.vm_cpu_history[uid].append(s.get('cpuUsage', 0.0))

    def evaluate_cluster(self, nodes, vms) -> Dict:
        state = {}
        for n in nodes:
            uid = n['uuid']
            lan_ip = n.get('lanIP', uid)
            is_excluded = lan_ip in self.config['EXCLUDE_NODE_IPS']
            vms_on_node = []
            
            for vm in vms:
                if vm.get('nodeUUID') == uid and vm.get('state') == 'RUNNING':
                    avg_cpu = mean(self.vm_cpu_history.get(vm['uuid'], [0]))
                    vms_on_node.append({
                        "uuid": vm['uuid'],
                        "name": vm.get('name', 'Unknown'),
                        "mem": vm.get('mem', 0),
                        "vcpus": vm.get('numVCPU', 1),
                        "avg_cpu": avg_cpu,
                        "tags": vm.get('tags', "")
                    })
            
            threads = n.get('numThreads', 1)
            allocated_vcpus = sum(vm['vcpus'] for vm in vms_on_node)
            is_usable = n.get('networkStatus') == 'ONLINE' and n.get('allowRunningVMs', True)
            
            state[uid] = {
                "uuid": uid,
                "name": lan_ip,
                "avg_cpu": mean(self.node_cpu_history.get(uid, [0])),
                "avg_ram": mean(self.node_ram_history.get(uid, [0])),
                "used_ram": n.get('totalMemUsageBytes', 0),
                "total_ram": n.get('memSize', 1),
                "threads": threads,
                "v_ratio": allocated_vcpus / threads,
                "is_usable": is_usable and not is_excluded,
                "is_excluded": is_excluded,
                "vms": sorted(vms_on_node, key=lambda x: x['avg_cpu'], reverse=True)
            }
        return state

    def select_candidate(self, state: Dict) -> Tuple:
        usable = [u for u in state.values() if u['is_usable']]
        if not usable:
            self.log(2, "No usable nodes found (all nodes may be excluded or offline).")
            return None, None, None, "NONE"

        peak_cpu_node = max(usable, key=lambda x: x['avg_cpu'])
        cpu_threshold = self.config['CPU_UPPER_THRESHOLD_PERCENT']
        
        primary = "CPU_FOCUS" if peak_cpu_node['avg_cpu'] >= cpu_threshold else "RAM_FOCUS"
        modes = [primary]
        if primary == "CPU_FOCUS":
            modes.append("RAM_FOCUS")

        for mode in modes:
            self.log(2, f"Evaluating {mode} (Peak Cluster CPU: {peak_cpu_node['avg_cpu']:.1f}%)")
            sources = sorted(usable, key=lambda x: x['avg_cpu'] if mode == "CPU_FOCUS" else (x['avg_ram'], x['v_ratio']), reverse=True)

            for src in sources:
                if mode == "CPU_FOCUS" and src['avg_cpu'] < cpu_threshold: continue
                if mode == "RAM_FOCUS" and src['avg_ram'] < self.config['RAM_UPPER_THRESHOLD_PERCENT']: continue

                for vm in src['vms']:
                    if f"node_{src['name'].split('.')[-1]}" in vm['tags']:
                        self.log(3, f"  - Skipping {vm['name']} (pinned to {src['name']})")
                        continue

                    cd_remaining = (self.config['VM_MOVE_COOLDOWN_MINUTES'] * 60) - (time.time() - self.vm_cooldowns.get(vm['uuid'], 0))
                    if cd_remaining > 0:
                        self.log(3, f"  - Skipping {vm['name']} (cooldown active)")
                        continue

                    # Anti-affinity sets for the candidate VM
                    vm_anti = {t.strip()[5:] for t in vm['tags'].split(',') if t.strip().startswith('anti_')}

                    targets = sorted(usable, key=lambda x: x['avg_ram'] if mode == "RAM_FOCUS" else x['avg_cpu'])
                    for tgt in targets:
                        if tgt['name'] == src['name']: continue

                        if tgt['uuid'] in self.reserved_nodes:
                            if time.time() < self.reserved_nodes[tgt['uuid']]:
                                self.log(3, f"  - Skipping target node {tgt['name']} (Reserved for impending predicted spike)")
                                continue
                            else:
                                del self.reserved_nodes[tgt['uuid']]

                        # Anti-affinity: skip if candidate's anti_X tag matches a VM name on target,
                        # or if any VM on target has anti_<candidate> pointing back at us
                        tgt_names = {v['name'] for v in tgt['vms']}
                        tgt_anti  = {t.strip()[5:] for v in tgt['vms'] for t in v['tags'].split(',') if t.strip().startswith('anti_')}
                        if vm_anti & tgt_names:
                            self.log(2, f"  - Skipping target {tgt['name']}: anti-affinity ({vm_anti & tgt_names})")
                            continue
                        if vm['name'] in tgt_anti:
                            self.log(2, f"  - Skipping target {tgt['name']}: anti-affinity (reverse block on {vm['name']})")
                            continue

                        p_ram = ((tgt['used_ram'] + vm['mem']) / tgt['total_ram']) * 100
                        p_ratio = (sum(v['vcpus'] for v in tgt['vms']) + vm['vcpus']) / tgt['threads']

                        if p_ram > self.config['RAM_LIMIT_PERCENT'] or p_ratio > self.config['MAX_VCPU_RATIO']:
                            continue

                        if mode == "CPU_FOCUS" and tgt['avg_cpu'] >= src['avg_cpu']: continue

                        self.log(2, f"Candidate found: {vm['name']} via {mode}")
                        return vm, src, tgt, mode
        return None, None, None, primary

    def find_affinity_violation(self, usable):
        """
        Highest-priority pass: detect and return one remediation move for any affinity violation.
        Bypasses migration cooldown and per-VM cooldown — affinity is always fixed first.

        Checks (in priority order):
          1. Anti-affinity: VM with anti_X tag sharing a node with VM named X
          2. Node-pin:      VM with node_X tag residing on the wrong node

        If the ideal target is at capacity, returns a 'make room' move instead —
        the lightest evictable VM on the target is moved out so the real fix
        can execute next cycle.

        Returns (vm, src_node, tgt_node, reason) or (None, None, None, None).
        """

        def _can_accept(tgt, vm):
            p_ram   = ((tgt['used_ram'] + vm['mem']) / tgt['total_ram']) * 100
            p_ratio = (sum(v['vcpus'] for v in tgt['vms']) + vm['vcpus']) / tgt['threads']
            return (p_ram   <= self.config['RAM_LIMIT_PERCENT'] and
                    p_ratio <= self.config['MAX_VCPU_RATIO'])

        def _anti_ok(vm_name, vm_anti, tgt):
            tgt_names = {v['name'] for v in tgt['vms']}
            tgt_anti  = {t.strip()[5:] for v in tgt['vms']
                         for t in v['tags'].split(',') if t.strip().startswith('anti_')}
            return not (vm_anti & tgt_names) and vm_name not in tgt_anti

        def _pinned_suffix(vm):
            for t in vm['tags'].split(','):
                t = t.strip()
                if t.startswith('node_'):
                    return t[5:]
            return None

        def _find_room_move(target_node):
            """Evict the lightest non-pinned VM from target_node to any valid destination."""
            for blocker in sorted(target_node['vms'], key=lambda v: v['mem']):
                if _pinned_suffix(blocker) == target_node['name'].split('.')[-1]:
                    continue  # Cannot evict a VM pinned to this node
                blocker_anti = {t.strip()[5:] for t in blocker['tags'].split(',')
                                if t.strip().startswith('anti_')}
                for dest in sorted(usable, key=lambda n: n['avg_cpu']):
                    if dest['uuid'] == target_node['uuid']:
                        continue
                    if not _anti_ok(blocker['name'], blocker_anti, dest):
                        continue
                    if not _can_accept(dest, blocker):
                        continue
                    return blocker, dest
            return None, None

        # ── 1. Anti-affinity violations ─────────────────────────────────────
        for src in usable:
            names_here = {v['name'] for v in src['vms']}
            for vm in src['vms']:
                vm_anti = {t.strip()[5:] for t in vm['tags'].split(',')
                           if t.strip().startswith('anti_')}
                conflicts = vm_anti & names_here
                if not conflicts:
                    continue

                self.log(1, f"[AFFINITY] VIOLATION: {vm['name']} shares {src['name']} "
                            f"with {conflicts} — remediating.")

                for tgt in sorted(usable, key=lambda n: n['avg_cpu']):
                    if tgt['uuid'] == src['uuid']:
                        continue
                    pin = _pinned_suffix(vm)
                    if pin and pin != tgt['name'].split('.')[-1]:
                        continue  # VM is also node-pinned — respect that constraint
                    if not _anti_ok(vm['name'], vm_anti, tgt):
                        continue
                    if _can_accept(tgt, vm):
                        return (vm, src, tgt,
                                f"Anti-affinity: {vm['name']} must not share node with {conflicts}")
                    blocker, room_tgt = _find_room_move(tgt)
                    if blocker:
                        return (blocker, tgt, room_tgt,
                                f"Making space for {vm['name']}")

                self.log(1, f"[AFFINITY] Cannot fix anti-affinity for {vm['name']} — "
                            f"no valid target available. Cluster may be physically full.")

        # ── 2. Node-pin violations ───────────────────────────────────────────
        for src in usable:
            src_suffix = src['name'].split('.')[-1]
            for vm in src['vms']:
                pin = _pinned_suffix(vm)
                if not pin or pin == src_suffix:
                    continue  # Not pinned, or already on correct node

                tgt = next((n for n in usable if n['name'].split('.')[-1] == pin), None)
                if not tgt:
                    self.log(1, f"[AFFINITY] Cannot fix pin for {vm['name']}: "
                                f"node_{pin} is not in the usable set.")
                    continue

                self.log(1, f"[AFFINITY] VIOLATION: {vm['name']} is on {src['name']} "
                            f"but pinned to {tgt['name']} — remediating.")

                vm_anti = {t.strip()[5:] for t in vm['tags'].split(',')
                           if t.strip().startswith('anti_')}
                if not _anti_ok(vm['name'], vm_anti, tgt):
                    self.log(1, f"[AFFINITY] Cannot fix pin for {vm['name']}: "
                                f"anti-affinity conflict on {tgt['name']}.")
                    continue
                if _can_accept(tgt, vm):
                    return (vm, src, tgt,
                            f"Pin violation: {vm['name']} belongs on node_{pin} ({tgt['name']})")
                blocker, room_tgt = _find_room_move(tgt)
                if blocker:
                    return (blocker, tgt, room_tgt,
                            f"Making space for {vm['name']}")
                self.log(1, f"[AFFINITY] Cannot make room on {tgt['name']} for {vm['name']}.")

        return None, None, None, None

    def _run_predictive_background(self, vms_snapshot, nodes_snapshot):
        """Runs the predictive engine in a background thread, then discards the snapshot."""
        try:
            from predictive_engine import get_proactive_migrations
            self._predictive_results = get_proactive_migrations(vms_snapshot, nodes_snapshot)
        except ImportError:
            self.log(0, "[PREDICTIVE ERROR] Could not load predictive_engine. Are 'pandas' and 'prophet' installed?")
            self._predictive_results = []
        except Exception as e:
            self.log(0, f"[PREDICTIVE ERROR] Engine failed: {e}")
            traceback.print_exc()
            self._predictive_results = []
        finally:
            del vms_snapshot, nodes_snapshot
            gc.collect()

    def start(self):
        mode_str = "DRY" if self.config['DRY_RUN'] else "LIVE"
        self.log(0, f"Balancer Start (Mode: {mode_str})")
        
        if self.config['PREDICTIVE_BALANCING_ENABLED']:
            self.log(0, "🔮 Predictive Balancing Module is ENABLED.")
            
        try:
            while True:
                # Hot-reload tunables every 60 seconds
                now_ts = time.time()
                if now_ts - self._last_config_check > 60:
                    self.reload_config()
                    self._last_config_check = now_ts

                if self.active_task:
                    status = self.client.get_task_status(self.active_task['tag'])
                    if status in ["COMPLETE", "ERROR"]:
                        self.log(0, f"Migration {status} for {self.active_task['vm']}")
                        self.active_task = None
                        self.last_migration_time = time.time()
                    time.sleep(10); continue

                try:
                    nodes, vms, stats = self.client.get_nodes(), self.client.get_vms(), self.client.get_vm_stats()
                    self.update_metrics(nodes, stats)
                except Exception as e:
                    self.log(0, f"API Fetch Error: {e}"); time.sleep(30); continue

                # Evaluate cluster state once — shared by affinity, predictive, and reactive engines
                state  = self.evaluate_cluster(nodes, vms)
                usable = [n for n in state.values() if n['is_usable']]

                # =================================================================
                # AFFINITY ENFORCEMENT — highest priority, bypasses all cooldowns
                # =================================================================
                aff_vm, aff_src, aff_tgt, aff_reason = self.find_affinity_violation(usable)
                if aff_vm:
                    info = f"{aff_vm['name']} | {aff_src['name']} -> {aff_tgt['name']} (Affinity: {aff_reason})"
                    if self.config['DRY_RUN']:
                        self.log(0, f"[AFFINITY DRY RUN] Would move {info}")
                        self.vm_cooldowns[aff_vm['uuid']] = time.time()
                        self.last_migration_time = time.time()
                    else:
                        self.log(0, f"[AFFINITY ACTION] {info}")
                        try:
                            task = self.client.migrate(aff_vm['uuid'], aff_tgt['uuid'])
                            self.active_task = {"tag": task['taskTag'], "vm": aff_vm['name']}
                            self.vm_cooldowns[aff_vm['uuid']] = time.time()
                            self.last_migration_time = time.time()
                        except Exception as e:
                            self.log(0, f"[AFFINITY ERROR] Migration rejected for {aff_vm['name']}: {e}")
                            self.vm_cooldowns[aff_vm['uuid']] = time.time()
                    aff_mode = "anti-affinity" if "anti-affinity" in aff_reason.lower() else "node-affinity"
                    self.migration_logger.log_event(
                        vm_name=aff_vm['name'], vm_uuid=aff_vm['uuid'],
                        source_ip=aff_src['name'], target_ip=aff_tgt['name'],
                        engine="Affinity", mode=aff_mode,
                        dry_run=self.config['DRY_RUN'], reason="affinity-violation"
                    )
                    time.sleep(self.config['SAMPLE_INTERVAL_SECONDS'])
                    continue

                # Non-affinity migration cooldown
                elapsed = time.time() - self.last_migration_time
                cluster_cd = self.config['MIGRATION_COOLDOWN_MINUTES'] * 60
                if elapsed < cluster_cd:
                    time.sleep(self.config['SAMPLE_INTERVAL_SECONDS']); continue

                # =================================================================
                # OPTIONAL: PREDICTIVE BALANCING
                # =================================================================
                if self.config['PREDICTIVE_BALANCING_ENABLED']:
                    applied_predictive = False
                    now = time.time()
                    lead_time_seconds = self.config['PREDICTIVE_LEAD_TIME_HOURS'] * 3600

                    # --- Store results from a completed forecast thread ---
                    if self._predictive_thread is not None and not self._predictive_thread.is_alive():
                        recommendations = self._predictive_results or []
                        self._predictive_results = None
                        self._predictive_thread = None
                        self._predictive_overrun_logged = False

                        if recommendations:
                            current_vms = {vm['uuid']: vm for vm in vms}
                            current_node_uuids = {n['uuid'] for n in nodes}
                            valid_recs = []
                            for rec in recommendations:
                                vm_current = current_vms.get(rec.vm_uuid)
                                if not vm_current:
                                    self.log(0, f"[PREDICTIVE] Skipping stale recommendation: VM '{rec.vm_name}' no longer exists in cluster.")
                                    continue
                                if vm_current.get('nodeUUID') != rec.source_node:
                                    self.log(0, f"[PREDICTIVE] Skipping stale recommendation: VM '{rec.vm_name}' has already moved to a different node.")
                                    continue
                                if rec.target_node not in current_node_uuids:
                                    self.log(0, f"[PREDICTIVE] Skipping stale recommendation: target node for '{rec.vm_name}' is no longer available.")
                                    continue
                                valid_recs.append(rec)

                            self._pending_predictive_recs = valid_recs
                            if valid_recs:
                                earliest = min(valid_recs, key=lambda r: r.peak_time)
                                earliest_str = time.strftime('%Y-%m-%d %H:%M', time.localtime(earliest.peak_time))
                                self.log(0, f"[PREDICTIVE] {len(valid_recs)} recommendation(s) queued. Earliest peak at {earliest_str}.")

                                # Reserve all source nodes immediately — don't wait for each migration
                                # to execute. This prevents the reactive engine from routing VMs back
                                # onto a node the Oracle just decided is going to be overloaded.
                                reservation_secs = self.config.get('PREDICTIVE_RESERVATION_MINUTES', 5) * 60
                                seen_sources = set()
                                for rec in valid_recs:
                                    if rec.source_node in seen_sources:
                                        continue
                                    seen_sources.add(rec.source_node)
                                    release_time = rec.peak_time + reservation_secs
                                    existing = self.reserved_nodes.get(rec.source_node, 0)
                                    if release_time > existing:
                                        self.reserved_nodes[rec.source_node] = release_time
                                        fmt = time.strftime('%Y-%m-%d %H:%M', time.localtime(release_time))
                                        self.log(0, f"[RESERVATION] Node {rec.source_ip} locked as target until {fmt} (Oracle: predicted overload)")

                    # --- Discard recommendations whose peak has already passed ---
                    self._pending_predictive_recs = [r for r in self._pending_predictive_recs if r.peak_time > now]

                    # --- Act on any recommendation whose peak is within the lead time window ---
                    ready_recs = [r for r in self._pending_predictive_recs if r.peak_time - now <= lead_time_seconds]
                    if ready_recs:
                        rec = ready_recs[0]
                        self._pending_predictive_recs.remove(rec)
                        info = f"{rec.vm_name} | {rec.source_ip} -> {rec.target_ip} (Predictive: {rec.reason})"

                        if self.config['DRY_RUN']:
                            self.log(0, f"[PREDICTIVE DRY RUN] Would move {info}")
                            self.vm_cooldowns[rec.vm_uuid] = time.time()
                            self.last_migration_time = time.time()
                        else:
                            self.log(0, f"[PREDICTIVE ACTION] Migrating {info}")
                            try:
                                task = self.client.migrate(rec.vm_uuid, rec.target_node)
                                self.active_task = {"tag": task['taskTag'], "vm": rec.vm_name}
                                self.vm_cooldowns[rec.vm_uuid] = time.time()
                                self.last_migration_time = time.time()
                            except Exception as e:
                                self.log(0, f"[PREDICTIVE ERROR] Migration request rejected for {rec.vm_name}: {e}")
                                self.vm_cooldowns[rec.vm_uuid] = time.time()
                                applied_predictive = False
                                continue

                        self.migration_logger.log_event(
                            vm_name=rec.vm_name, vm_uuid=rec.vm_uuid,
                            source_ip=rec.source_ip, target_ip=rec.target_ip,
                            engine="predictive", mode="PREDICTIVE",
                            dry_run=self.config['DRY_RUN'], reason=rec.reason
                        )

                        reservation_secs = self.config.get('PREDICTIVE_RESERVATION_MINUTES', 5) * 60
                        release_time = rec.peak_time + reservation_secs
                        existing = self.reserved_nodes.get(rec.source_node, 0)
                        if release_time > existing:
                            self.reserved_nodes[rec.source_node] = release_time
                            formatted_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(release_time))
                            self.log(0, f"[RESERVATION] Node {rec.source_ip} locked from receiving VMs until {formatted_time}")
                        applied_predictive = True

                    # --- Start a new forecast thread if interval has elapsed ---
                    jitter = random.uniform(0, self.config['PREDICTIVE_INTERVAL_SECONDS'] * 0.1)
                    if time.time() - self.last_predictive_run > (self.config['PREDICTIVE_INTERVAL_SECONDS'] + jitter):
                        if self._predictive_thread is not None and self._predictive_thread.is_alive():
                            if not self._predictive_overrun_logged:
                                self.log(0, "[PREDICTIVE] WARNING: Previous forecast is still running — skipping this interval.")
                                self._predictive_overrun_logged = True
                        else:
                            self.log(0, "[PREDICTIVE] Starting background forecast thread.")
                            vms_snapshot = list(vms)
                            nodes_snapshot = list(nodes)
                            self._predictive_thread = threading.Thread(
                                target=self._run_predictive_background,
                                args=(vms_snapshot, nodes_snapshot),
                                daemon=True,
                                name="PredictiveEngine"
                            )
                            self._predictive_thread.start()
                        self.last_predictive_run = time.time()

                    if self.active_task or (self.config['DRY_RUN'] and applied_predictive):
                        time.sleep(self.config['SAMPLE_INTERVAL_SECONDS'])
                        continue

                # =================================================================
                # REACTIVE BALANCING
                # =================================================================
                first_uid = nodes[0]['uuid'] if nodes else None
                if first_uid and len(self.node_cpu_history.get(first_uid, [])) < self.max_history:
                    self.log(2, f"Buffer: {len(self.node_cpu_history[first_uid])}/{self.max_history}")
                    time.sleep(self.config['SAMPLE_INTERVAL_SECONDS']); continue

                vm, src, tgt, mode = self.select_candidate(state)
                
                if vm:
                    info = f"{vm['name']} | {src['name']} -> {tgt['name']} ({mode})"
                    if self.config['DRY_RUN']:
                        self.log(0, f"[REACTIVE DRY RUN] Would move {info}")
                        self.last_migration_time = time.time()
                        self.vm_cooldowns[vm['uuid']] = time.time()
                        self.node_cpu_history.clear()
                    else:
                        self.log(0, f"[REACTIVE ACTION] Migrating {info}")
                        try:
                            task = self.client.migrate(vm['uuid'], tgt['uuid'])
                            self.active_task = {"tag": task['taskTag'], "vm": vm['name']}
                            self.vm_cooldowns[vm['uuid']] = time.time()
                        except Exception as e:
                            self.log(0, f"[REACTIVE ERROR] Migration request rejected for {vm['name']}: {e}")
                            self.vm_cooldowns[vm['uuid']] = time.time()
                            time.sleep(self.config['SAMPLE_INTERVAL_SECONDS'])
                            continue

                    # Log the event to InfluxDB for the dashboard
                    self.migration_logger.log_event(
                        vm_name=vm['name'], vm_uuid=vm['uuid'],
                        source_ip=src['name'], target_ip=tgt['name'],
                        engine="reactive", mode=mode,
                        dry_run=self.config['DRY_RUN'],
                        reason=f"Reactive {mode} balancing"
                    )
                    
                time.sleep(self.config['SAMPLE_INTERVAL_SECONDS'])
        except KeyboardInterrupt: self.log(0, "Shutdown requested.")
        finally:
            self.client.close()
            self.migration_logger.close()

def _handle_sigterm(*_):
    raise KeyboardInterrupt()

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    config_db.seed_from_env()
    CONFIG = config_db.get_all()
    client = HyperCoreApiClient(SC_HOST, SC_USERNAME, SC_PASSWORD, SC_VERIFY_SSL)
    migration_logger = MigrationLogger(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
    if client.login():
        LoadBalancer(client, CONFIG, migration_logger).start()
    else:
        sys.exit("Login failed. Check credentials.")