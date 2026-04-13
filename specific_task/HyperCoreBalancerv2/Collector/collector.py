import os
import time
import requests
import warnings
from urllib3.exceptions import InsecureRequestWarning
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import config_db

# Configurables — stay in .env, require restart to change
SC_HOST = os.getenv('SC_HOST')
SC_USER = os.getenv('SC_USERNAME')
SC_PASS = os.getenv('SC_PASSWORD')
SC_VERIFY_SSL = os.getenv('SC_VERIFY_SSL', 'false').lower() in ('true', '1', 'yes', 'y')
if not SC_VERIFY_SSL:
    warnings.simplefilter('ignore', InsecureRequestWarning)
INFLUX_URL = os.getenv('INFLUX_URL')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET')

# Tunables are seeded on startup and re-read from config_db each loop iteration


def fetch_data(session, endpoint):
    res = session.get(f"{SC_HOST}{endpoint}", timeout=15)
    res.raise_for_status()
    return res.json()


def fetch_safe(session, endpoint, default=None):
    """Fetch with graceful fallback — returns default on failure instead of crashing."""
    try:
        return fetch_data(session, endpoint)
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Warning: Failed to fetch {endpoint}: {e}")
        return default if default is not None else []


def do_login(session):
    print(f"[{time.strftime('%H:%M:%S')}] Authenticating with HyperCore API...")
    session.cookies.clear()
    res = session.post(f"{SC_HOST}/login", json={"username": SC_USER, "password": SC_PASS}, timeout=10)
    res.raise_for_status()


def do_logout(session):
    print(f"[{time.strftime('%H:%M:%S')}] Logging out from HyperCore API...")
    try:
        session.post(f"{SC_HOST}/logout", timeout=5)
    except Exception as e:
        print(f"Logout failed or already disconnected: {e}")


def write_with_retry(write_api, bucket, points, max_retries=3, retry_delay=5):
    """Attempts to write points to InfluxDB with retries on transient failures."""
    for attempt in range(1, max_retries + 1):
        try:
            write_api.write(bucket=bucket, record=points)
            return True
        except Exception as e:
            if attempt < max_retries:
                print(f"[{time.strftime('%H:%M:%S')}] InfluxDB write failed (attempt {attempt}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] InfluxDB write failed after {max_retries} attempts: {e}. Dropping {len(points)} points.")
                return False


def collect_fast(session, nodes, vms, vm_stats, node_info_map):
    """Collects high-frequency metrics: node performance, VM performance, drive health, VSD I/O."""
    points = []

    # =========================================================================
    # NODES — expanded with system memory, capacity, network status
    # =========================================================================
    for node in nodes:
        node_uuid = node.get('uuid', 'unknown')
        peer_id = str(node.get('peerID', 'unknown'))
        lan_ip = node.get('lanIP', 'unknown')
        node_info_map[node_uuid] = {'peer_id': peer_id, 'lan_ip': lan_ip}

        p = Point("node_metrics") \
            .tag("node_uuid", node_uuid) \
            .tag("node_peer_id", peer_id) \
            .tag("node_lan_ip", lan_ip) \
            .field("cpu_usage", float(node.get('cpuUsage', 0))) \
            .field("total_mem_usage_bytes", int(node.get('totalMemUsageBytes', 0))) \
            .field("system_mem_usage_bytes", int(node.get('systemMemUsageBytes', 0))) \
            .field("mem_usage_percentage", float(node.get('memUsagePercentage', 0))) \
            .field("mem_size", int(node.get('memSize', 0))) \
            .field("capacity_bytes", int(node.get('capacity', 0))) \
            .field("num_threads", int(node.get('numThreads', 0))) \
            .field("num_cores", int(node.get('numCores', 0))) \
            .field("num_sockets", int(node.get('numSockets', 0))) \
            .field("network_online", node.get('networkStatus') == 'ONLINE') \
            .field("virtualization_online", bool(node.get('virtualizationOnline', False))) \
            .field("allow_running_vms", bool(node.get('allowRunningVMs', True)))
        points.append(p)

        # --- Physical Drives — expanded ---
        for drive in node.get('drives', []):
            p_drive = Point("drive_metrics") \
                .tag("node_uuid", node_uuid) \
                .tag("node_peer_id", peer_id) \
                .tag("drive_uuid", drive.get('uuid', 'unknown')) \
                .tag("drive_slot", str(drive.get('slot', 'unknown'))) \
                .tag("drive_type", str(drive.get('type', 'unknown'))) \
                .tag("drive_serial", drive.get('serialNumber', 'unknown')) \
                .field("capacity_bytes", int(drive.get('capacityBytes', 0))) \
                .field("used_bytes", int(drive.get('usedBytes', 0))) \
                .field("error_count", int(drive.get('errorCount', 0))) \
                .field("reallocated_sectors", int(drive.get('reallocatedSectors', 0))) \
                .field("is_healthy", bool(drive.get('isHealthy', False)))

            if drive.get('hasTemperature'):
                p_drive.field("temperature", int(drive.get('temperature', 0)))
            if drive.get('hasMaxTemperature'):
                p_drive.field("max_temperature", int(drive.get('maxTemperature', 0)))
            if drive.get('hasTemperatureThreshold'):
                p_drive.field("temperature_threshold", int(drive.get('temperatureThreshold', 0)))

            points.append(p_drive)

    # =========================================================================
    # VM INFO MAP — used by stats and block/net device sections
    # =========================================================================
    vm_info_map = {}
    vsd_info_map = {}

    for vm in vms:
        vm_uuid = vm.get('uuid')
        current_node_uuid = vm.get('nodeUUID', 'unknown')
        node_info = node_info_map.get(current_node_uuid, {'peer_id': 'unknown', 'lan_ip': 'unknown'})

        raw_tags = vm.get('tags', '')
        first_tag = raw_tags.split(',')[0].strip() if raw_tags else ''

        net_devs = vm.get('netDevs', [])
        primary_vlan = str(net_devs[0].get('vlan', 0)) if net_devs else '0'

        vm_info_map[vm_uuid] = {
            "name": vm.get('name', 'Unknown'),
            "node_uuid": current_node_uuid,
            "node_peer_id": node_info['peer_id'],
            "first_tag": first_tag,
            "primary_vlan": primary_vlan,
            "state": vm.get('state', 'UNKNOWN'),
            "num_vcpu": vm.get('numVCPU', 0),
            "mem": vm.get('mem', 0),
            "os": vm.get('operatingSystem', ''),
            "description": vm.get('description', ''),
            "snapshot_count": len(vm.get('snapUUIDs', [])),
        }

        for block_dev in vm.get('blockDevs', []):
            bd_uuid = block_dev.get('uuid')
            bd_name = block_dev.get('name', '')
            bd_type = block_dev.get('type', 'unknown')
            vsd_info_map[bd_uuid] = {
                "name": bd_name if bd_name else bd_type,
                "type": bd_type,
                "capacity": block_dev.get('capacity', 0),
                "allocation": block_dev.get('allocation', 0),
                "physical": block_dev.get('physical', 0),
                "slot": block_dev.get('slot', -1),
                "cache_mode": block_dev.get('cacheMode', 'unknown'),
            }

    # =========================================================================
    # VM METRICS — performance (from VirDomainStats)
    # =========================================================================
    for stat in vm_stats:
        vm_uuid = stat.get('uuid')
        vm_info = vm_info_map.get(vm_uuid, {
            "name": "Unknown", "node_uuid": "unknown", "node_peer_id": "unknown",
            "first_tag": "", "primary_vlan": "0"
        })

        p_vm = Point("vm_metrics") \
            .tag("vm_uuid", vm_uuid) \
            .tag("vm_name", vm_info['name']) \
            .tag("node_uuid", vm_info['node_uuid']) \
            .tag("node_peer_id", vm_info['node_peer_id']) \
            .tag("vm_tag", vm_info['first_tag']) \
            .tag("vm_vlan", vm_info['primary_vlan']) \
            .field("cpu_usage", float(stat.get('cpuUsage', 0))) \
            .field("rx_bit_rate", int(stat.get('rxBitRate', 0))) \
            .field("tx_bit_rate", int(stat.get('txBitRate', 0)))
        points.append(p_vm)

        # --- VSD I/O Metrics ---
        for vsd in stat.get('vsdStats', []):
            vsd_uuid = vsd.get('uuid')
            rates = vsd.get('rates', [])
            vsd_info = vsd_info_map.get(vsd_uuid, {"name": "unknown", "type": "unknown"})

            if rates:
                r = rates[0]
                p_vsd = Point("vsd_metrics") \
                    .tag("vm_uuid", vm_uuid) \
                    .tag("vm_name", vm_info['name']) \
                    .tag("node_uuid", vm_info['node_uuid']) \
                    .tag("node_peer_id", vm_info['node_peer_id']) \
                    .tag("vsd_uuid", vsd_uuid) \
                    .tag("vsd_name", vsd_info['name']) \
                    .tag("vsd_type", vsd_info['type']) \
                    .field("read_kbps", int(r.get('readKibibytesPerSecond', 0))) \
                    .field("write_kbps", int(r.get('writeKibibytesPerSecond', 0))) \
                    .field("read_latency_us", int(r.get('meanReadLatencyMicroseconds', 0))) \
                    .field("write_latency_us", int(r.get('meanWriteLatencyMicroseconds', 0))) \
                    .field("read_iops", int(r.get('millireadsPerSecond', 0)) / 1000) \
                    .field("write_iops", int(r.get('milliwritesPerSecond', 0)) / 1000) \
                    .field("mean_read_size_bytes", int(r.get('meanReadSizeBytes', 0))) \
                    .field("mean_write_size_bytes", int(r.get('meanWriteSizeBytes', 0)))
                points.append(p_vsd)

    # =========================================================================
    # VM INVENTORY — state snapshot per cycle (for tracking power state changes,
    # resource allocation over time, snapshot counts, etc.)
    # =========================================================================
    for vm_uuid, info in vm_info_map.items():
        vsd_info = vsd_info_map  # for block device capacity lookup
        p_inv = Point("vm_inventory") \
            .tag("vm_uuid", vm_uuid) \
            .tag("vm_name", info['name']) \
            .tag("node_uuid", info['node_uuid']) \
            .tag("node_peer_id", info['node_peer_id']) \
            .tag("vm_tag", info['first_tag']) \
            .tag("vm_vlan", info['primary_vlan']) \
            .tag("vm_state", info.get('state', 'UNKNOWN')) \
            .tag("vm_os", info.get('os', '')) \
            .field("num_vcpu", int(info.get('num_vcpu', 0))) \
            .field("mem_bytes", int(info.get('mem', 0))) \
            .field("snapshot_count", int(info.get('snapshot_count', 0)))
        points.append(p_inv)

    # =========================================================================
    # BLOCK DEVICE INVENTORY — capacity and allocation tracking
    # =========================================================================
    for bd_uuid, bd_info in vsd_info_map.items():
        p_bd = Point("block_device_metrics") \
            .tag("vsd_uuid", bd_uuid) \
            .tag("vsd_name", bd_info['name']) \
            .tag("vsd_type", bd_info['type']) \
            .tag("cache_mode", bd_info.get('cache_mode', 'unknown')) \
            .field("capacity_bytes", int(bd_info.get('capacity', 0))) \
            .field("allocation_bytes", int(bd_info.get('allocation', 0))) \
            .field("physical_bytes", int(bd_info.get('physical', 0))) \
            .field("slot", int(bd_info.get('slot', -1)))
        points.append(p_bd)

    return points, vm_info_map


def collect_slow(session, node_info_map):
    """Collects low-frequency data: snapshots, replication, cluster info, conditions."""
    points = []

    # =========================================================================
    # CLUSTER INFO — version tracking, name
    # =========================================================================
    cluster = fetch_safe(session, "/Cluster")
    for c in cluster:
        p = Point("cluster_metrics") \
            .tag("cluster_uuid", c.get('uuid', 'unknown')) \
            .tag("cluster_name", c.get('clusterName', 'unknown')) \
            .field("icos_version", c.get('icosVersion', 'unknown'))
        points.append(p)

    # =========================================================================
    # SNAPSHOTS — per-VM snapshot tracking
    # =========================================================================
    snapshots = fetch_safe(session, "/VirDomainSnapshot")
    snap_by_vm = {}
    for snap in snapshots:
        dom_uuid = snap.get('domainUUID', 'unknown')
        if dom_uuid not in snap_by_vm:
            snap_by_vm[dom_uuid] = {'count': 0, 'total_block_diff': 0, 'latest_timestamp': 0}
        snap_by_vm[dom_uuid]['count'] += 1
        snap_by_vm[dom_uuid]['total_block_diff'] += snap.get('blockCountDiff', 0)
        ts = snap.get('timestamp', 0)
        if ts > snap_by_vm[dom_uuid]['latest_timestamp']:
            snap_by_vm[dom_uuid]['latest_timestamp'] = ts

    for vm_uuid, snap_info in snap_by_vm.items():
        p = Point("snapshot_metrics") \
            .tag("vm_uuid", vm_uuid) \
            .field("snapshot_count", snap_info['count']) \
            .field("total_block_diff", snap_info['total_block_diff']) \
            .field("latest_snapshot_timestamp", snap_info['latest_timestamp'])
        points.append(p)

    # =========================================================================
    # REPLICATION — progress tracking
    # =========================================================================
    replications = fetch_safe(session, "/VirDomainReplication")
    for rep in replications:
        progress = rep.get('progress', {})
        p = Point("replication_metrics") \
            .tag("replication_uuid", rep.get('uuid', 'unknown')) \
            .tag("source_vm_uuid", rep.get('sourceDomainUUID', 'unknown')) \
            .tag("enabled", str(rep.get('enable', False)).lower()) \
            .field("label", rep.get('label', '')) \
            .field("percent_complete", int(progress.get('percentComplete', 0))) \
            .field("block_count_diff", int(progress.get('blockCountDiff', 0))) \
            .field("block_count_sent", int(progress.get('blockCountSent', 0)))
        points.append(p)

    # =========================================================================
    # CONDITIONS — cluster health alerts
    # =========================================================================
    conditions = fetch_safe(session, "/Condition")
    for cond in conditions:
        # Only collect user-visible, active conditions to avoid noise
        if not cond.get('userVisible', False):
            continue
        p = Point("condition_metrics") \
            .tag("condition_name", cond.get('name', 'unknown')) \
            .tag("node_uuid", cond.get('nodeUUID', '')) \
            .tag("node_lan_ip", cond.get('nodeLANIP', '')) \
            .tag("severity", cond.get('severity', 'INFO')) \
            .field("value", bool(cond.get('value', False))) \
            .field("is_ok", bool(cond.get('isOK', True))) \
            .field("is_expired", bool(cond.get('isExpired', False))) \
            .field("description", cond.get('description', ''))
        points.append(p)

    return points


INFLUX_PRIMARY_RETENTION = int(os.getenv('INFLUX_PRIMARY_RETENTION', 365))


def set_bucket_retention(influx_client):
    """Sets retention on the primary bucket once at startup via the InfluxDB API."""
    # Wait for InfluxDB to be ready
    print(f"[{time.strftime('%H:%M:%S')}] Waiting for InfluxDB to be ready...")
    for attempt in range(1, 61):
        try:
            influx_client.ping()
            print(f"[{time.strftime('%H:%M:%S')}] InfluxDB is ready.")
            break
        except Exception:
            if attempt % 10 == 0:
                print(f"[{time.strftime('%H:%M:%S')}] Still waiting for InfluxDB... ({attempt}s)")
            time.sleep(1)
    else:
        print(f"[{time.strftime('%H:%M:%S')}] WARNING: InfluxDB not reachable after 60s, skipping retention config.")
        return

    retention_seconds = INFLUX_PRIMARY_RETENTION * 86400
    try:
        buckets_api = influx_client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(INFLUX_BUCKET)
        if bucket:
            bucket.retention_rules = [{"type": "expire", "everySeconds": retention_seconds}]
            buckets_api.update_bucket(bucket=bucket)
            print(f"[{time.strftime('%H:%M:%S')}] Bucket '{INFLUX_BUCKET}' retention set to {INFLUX_PRIMARY_RETENTION} days.")
        else:
            print(f"[{time.strftime('%H:%M:%S')}] WARNING: Bucket '{INFLUX_BUCKET}' not found, skipping retention config.")
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] WARNING: Could not set bucket retention: {e}")


def main():
    print("Start HyperCore Collector (Extended)...")

    config_db.seed_from_env()

    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    # Set retention policy on startup
    set_bucket_retention(influx_client)

    session = requests.Session()
    session.verify = SC_VERIFY_SSL

    try:
        do_login(session)
    except Exception as e:
        print(f"Initial login failed: {e}")

    last_slow_poll = 0
    node_info_map = {}

    try:
        while True:
            # Re-read tunables each iteration — picks up live dashboard changes
            poll_interval      = config_db.get('POLL_INTERVAL')       or 30
            slow_poll_interval = config_db.get('SLOW_POLL_INTERVAL')  or 300
            write_retries      = config_db.get('INFLUX_WRITE_RETRIES') or 3
            retry_delay        = config_db.get('INFLUX_RETRY_DELAY')   or 5

            try:
                # --- Fast poll: performance metrics ---
                nodes = fetch_data(session, "/Node")
                vms = fetch_data(session, "/VirDomain")
                vm_stats = fetch_data(session, "/VirDomainStats")

                fast_points, vm_info_map = collect_fast(session, nodes, vms, vm_stats, node_info_map)

                # --- Slow poll: inventory & health ---
                slow_points = []
                if time.time() - last_slow_poll >= slow_poll_interval:
                    slow_points = collect_slow(session, node_info_map)
                    last_slow_poll = time.time()

                all_points = fast_points + slow_points
                if write_with_retry(write_api, INFLUX_BUCKET, all_points, write_retries, retry_delay):
                    slow_note = f" + {len(slow_points)} slow" if slow_points else ""
                    print(f"[{time.strftime('%H:%M:%S')}] Wrote {len(all_points)} datapoints ({len(fast_points)} fast{slow_note}).")

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    print(f"[{time.strftime('%H:%M:%S')}] Session expired (401). Re-authenticating...")
                    try:
                        do_login(session)
                    except Exception as login_err:
                        print(f"Re-authentication failed: {login_err}")
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] HTTP Error: {e}")
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] Unexpected Error: {e}")

            time.sleep(poll_interval)

    finally:
        do_logout(session)


if __name__ == "__main__":
    main()