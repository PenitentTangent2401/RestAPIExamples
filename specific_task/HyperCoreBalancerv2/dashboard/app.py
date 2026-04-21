"""
HyperCore Dashboard API
========================
A lightweight Flask API that queries InfluxDB and serves data to the frontend.

KEY DESIGN: Module Discovery
-----------------------------
Any new container in the stack just needs to write data to InfluxDB with a
distinct measurement name. The /api/modules endpoint auto-discovers all
measurements in the bucket, so the frontend can dynamically list and
display data from new modules without code changes.

Endpoints:
    GET /api/nodes          - Current node CPU/RAM metrics (last 1h)
    GET /api/drives         - Physical drive health & usage, grouped by drive (last 1h)
    GET /api/vms            - Current VM CPU metrics (last 1h)
    GET /api/vsds           - Virtual storage device I/O, grouped by VSD (last 1h)
    GET /api/migrations     - Migration event log (last 7d, configurable)
    GET /api/modules        - Auto-discovered measurement names in the bucket
    GET /api/query/<measurement> - Generic query for any measurement
    GET /                   - Serves the dashboard HTML
"""

import os
import json
import re
from functools import wraps
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
from influxdb_client import InfluxDBClient
import config_db

app = Flask(__name__, static_folder='static')
CORS(app)

# Seed tunables DB from env vars on first run (no-op if already seeded)
config_db.seed_from_env()

# InfluxDB Configuration (configurables — stay in .env, require restart to change)
INFLUX_URL = os.getenv('INFLUX_URL', 'http://influxdb:8086')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', 'super-secret-auth-token')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'hypercore')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', 'metrics')

# Fallback stale threshold — live value is read from config_db per request
STALE_SECONDS = int(os.getenv('SC_DASHBOARD_STALE_SECONDS', 120))

# Basic auth credentials — if either is unset, auth is disabled (dev mode)
DASHBOARD_USER = os.getenv('SC_DASHBOARD_USER', '')
DASHBOARD_PASSWORD = os.getenv('SC_DASHBOARD_PASSWORD', '')


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_USER or not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != DASHBOARD_USER or auth.password != DASHBOARD_PASSWORD:
            return Response(
                'Authentication required.',
                401,
                {'WWW-Authenticate': 'Basic realm="HyperCore Dashboard"'}
            )
        return f(*args, **kwargs)
    return decorated


def get_influx_client():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def is_stale(history_list):
    """Returns True if the most recent data point is older than STALE_SECONDS."""
    if not history_list:
        return True
    stale_secs = config_db.get('STALE_SECONDS') or STALE_SECONDS
    latest_time = max(h['time'] for h in history_list)
    latest_dt = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))
    return (datetime.now(timezone.utc) - latest_dt).total_seconds() > stale_secs


# =============================================================================
# API: Node Metrics (last 1 hour, 1-minute windows)
# =============================================================================
@app.route('/api/nodes')
@require_auth
def get_nodes():
    range_hours = request.args.get('hours', '1')
    client = get_influx_client()
    try:
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{range_hours}h)
          |> filter(fn: (r) => r["_measurement"] == "node_metrics")
          |> filter(fn: (r) => r["_field"] != "network_online" and r["_field"] != "virtualization_online" and r["_field"] != "allow_running_vms")
          |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        nodes = {}
        for table in result:
            for record in table.records:
                node_uuid = record.values.get('node_uuid', 'unknown')
                peer_id = record.values.get('node_peer_id', 'unknown')
                
                if node_uuid not in nodes:
                    nodes[node_uuid] = {
                        'uuid': node_uuid,
                        'peer_id': peer_id,
                        'history': []
                    }
                
                nodes[node_uuid]['history'].append({
                    'time': record.get_time().isoformat(),
                    'cpu_usage': record.values.get('cpu_usage'),
                    'mem_usage_bytes': record.values.get('total_mem_usage_bytes')
                })
        
        # Filter out nodes that haven't reported recently
        active = [n for n in nodes.values() if not is_stale(n['history'])]
        return jsonify(active)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: VM Metrics (last 1 hour, 1-minute windows)
# =============================================================================
@app.route('/api/vms')
@require_auth
def get_vms():
    range_hours = request.args.get('hours', '1')
    client = get_influx_client()
    try:
        query = f'''
        vmMetrics =
          from(bucket: "metrics")
            |> range(start: -{range_hours}h)
            |> filter(fn: (r) => r._measurement == "vm_metrics")
            |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")

        vmInventory =
          from(bucket: "metrics")
            |> range(start: -{range_hours}h)
            |> filter(fn: (r) =>
                r._measurement == "vm_inventory" and
                r._field == "num_vcpu" or
                r._field == "mem_bytes"
            )
            |> last()
            |> pivot(rowKey: ["vm_uuid"], columnKey: ["_field"], valueColumn: "_value")
            |> keep(columns: ["vm_uuid", "num_vcpu", "mem_bytes"])

        join(
          tables: {{m: vmMetrics, i: vmInventory}},
          on: ["vm_uuid"],
          method: "inner"
        )
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        vms = {}
        for table in result:
            for record in table.records:
                vm_uuid = record.values.get('vm_uuid', 'unknown')
                vm_name = record.values.get('vm_name', 'Unknown')
                node_uuid = record.values.get('node_uuid', 'unknown')
                
                if vm_uuid not in vms:
                    vms[vm_uuid] = {
                        'uuid': vm_uuid,
                        'name': vm_name,
                        'node_uuid': node_uuid,
                        'node_peer_id': record.values.get('node_peer_id', 'unknown'),
                        'tag': record.values.get('vm_tag', ''),
                        'vlan': record.values.get('vm_vlan', '0'),
                        'vcpus': record.values.get('num_vcpu', '0'),
                        'memory': record.values.get('mem_bytes', '0'),
                        'history': []
                    }
                
                vms[vm_uuid]['history'].append({
                    'time': record.get_time().isoformat(),
                    'cpu_usage': record.values.get('cpu_usage'),
                    'rx_bit_rate': record.values.get('rx_bit_rate'),
                    'tx_bit_rate': record.values.get('tx_bit_rate')
                })
        
        # Filter out VMs that haven't reported recently (deleted/stopped)
        active = [v for v in vms.values() if not is_stale(v['history'])]
        return jsonify(active)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: Drive Metrics (physical drives, nested under nodes)
# =============================================================================
@app.route('/api/drives')
@require_auth
def get_drives():
    range_hours = request.args.get('hours', '1')
    client = get_influx_client()
    try:
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{range_hours}h)
          |> filter(fn: (r) => r["_measurement"] == "drive_metrics")
          |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        drives = {}
        for table in result:
            for record in table.records:
                drive_uuid = record.values.get('drive_uuid', 'unknown')
                
                if drive_uuid not in drives:
                    drives[drive_uuid] = {
                        'uuid': drive_uuid,
                        'node_uuid': record.values.get('node_uuid', 'unknown'),
                        'node_peer_id': record.values.get('node_peer_id', 'unknown'),
                        'slot': record.values.get('drive_slot', '?'),
                        'history': []
                    }
                
                entry = {
                    'time': record.get_time().isoformat(),
                    'used_bytes': record.values.get('used_bytes'),
                    'error_count': record.values.get('error_count'),
                    'is_healthy': record.values.get('is_healthy'),
                }
                temp = record.values.get('temperature')
                if temp is not None:
                    entry['temperature'] = temp
                drives[drive_uuid]['history'].append(entry)
        
        # Filter out drives that haven't reported recently
        active = [d for d in drives.values() if not is_stale(d['history'])]
        return jsonify(active)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: VSD Metrics (virtual storage devices, nested under VMs)
# =============================================================================
@app.route('/api/vsds')
@require_auth
def get_vsds():
    range_hours = request.args.get('hours', '1')
    client = get_influx_client()
    try:
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{range_hours}h)
          |> filter(fn: (r) => r["_measurement"] == "vsd_metrics")
          |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        vsds = {}
        for table in result:
            for record in table.records:
                vsd_uuid = record.values.get('vsd_uuid', 'unknown')
                
                if vsd_uuid not in vsds:
                    vsds[vsd_uuid] = {
                        'uuid': vsd_uuid,
                        'name': record.values.get('vsd_name', 'unknown'),
                        'type': record.values.get('vsd_type', 'unknown'),
                        'vm_uuid': record.values.get('vm_uuid', 'unknown'),
                        'vm_name': record.values.get('vm_name', 'Unknown'),
                        'node_uuid': record.values.get('node_uuid', 'unknown'),
                        'history': []
                    }
                
                vsds[vsd_uuid]['history'].append({
                    'time': record.get_time().isoformat(),
                    'read_kbps': record.values.get('read_kbps'),
                    'write_kbps': record.values.get('write_kbps'),
                    'read_latency_us': record.values.get('read_latency_us'),
                    'write_latency_us': record.values.get('write_latency_us')
                })
        
        # Filter out VSDs that haven't reported recently
        active = [v for v in vsds.values() if not is_stale(v['history'])]
        return jsonify(active)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: Migration Events (last 7 days by default)
# =============================================================================
@app.route('/api/migrations')
@require_auth
def get_migrations():
    range_days = request.args.get('days', '7')
    client = get_influx_client()
    try:
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{range_days}d)
          |> filter(fn: (r) => r["_measurement"] == "migration_events")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)

        events = []
        for table in result:
            for record in table.records:
                events.append({
                    'time': record.get_time().isoformat(),
                    'vm_name': record.values.get('vm_name', 'Unknown'),
                    'vm_uuid': record.values.get('vm_uuid', ''),
                    'source_ip': record.values.get('source_ip', ''),
                    'target_ip': record.values.get('target_ip', ''),
                    'engine': record.values.get('engine', ''),
                    'mode': record.values.get('mode', ''),
                    'dry_run': record.values.get('dry_run', 'true'),
                    'reason': record.values.get('reason', '')
                })

        events.sort(key=lambda e: e['time'], reverse=True)
        return jsonify(events[:50])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: Module Discovery
# =============================================================================
# This is the key endpoint for extensibility. It queries InfluxDB for all
# measurement names in the bucket. When you add a new container that writes
# to a new measurement (e.g. "backup_events", "snapshot_metrics"), it shows
# up here automatically — no frontend code changes needed.
# =============================================================================
@app.route('/api/modules')
@require_auth
def get_modules():
    client = get_influx_client()
    try:
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{INFLUX_BUCKET}")
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        measurements = []
        for table in result:
            for record in table.records:
                measurements.append(record.get_value())
        
        return jsonify({
            'measurements': sorted(measurements),
            'description': 'All data sources writing to InfluxDB. Add a new container that writes to a new measurement and it appears here automatically.'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: Generic Measurement Query
# =============================================================================
# This lets the frontend query ANY measurement dynamically. When a new module
# appears in /api/modules, the frontend can immediately query its data without
# any backend changes.
# =============================================================================
@app.route('/api/query/<measurement>')
@require_auth
def query_measurement(measurement):
    try:
        range_hours = max(1, min(int(request.args.get('hours', '1')), 8760))
        limit       = max(1, min(int(request.args.get('limit', '100')), 10000))
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid hours or limit parameter'}), 400
    client = get_influx_client()
    try:
        # Sanitize measurement name (alphanumeric + underscores only)
        if not re.match(r'^[a-zA-Z0-9_]+$', measurement):
            return jsonify({'error': 'Invalid measurement name'}), 400

        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -{range_hours}h)
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
        '''
        result = client.query_api().query(org=INFLUX_ORG, query=query)
        
        records = []
        for table in result:
            for record in table.records:
                row = {'time': record.get_time().isoformat()}
                for key, value in record.values.items():
                    if key not in ('result', 'table', '_start', '_stop', '_time', '_measurement'):
                        row[key] = value
                records.append(row)
        
        return jsonify(records)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        client.close()


# =============================================================================
# API: Tunables Config (live read/write)
# =============================================================================
@app.route('/api/config', methods=['GET'])
@require_auth
def get_config():
    """Return all tunables with metadata for the settings UI."""
    try:
        return jsonify(config_db.get_with_meta())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config', methods=['POST'])
@require_auth
def post_config():
    """Update one or more tunables. Accepts {key: value, ...} JSON body."""
    try:
        updates = request.get_json(force=True)
        if not isinstance(updates, dict):
            return jsonify({'error': 'Expected a JSON object'}), 400
        config_db.save_many(updates)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# Serve the frontend
# =============================================================================
@app.route('/')
@require_auth
def index():
    return send_from_directory('static', 'index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)