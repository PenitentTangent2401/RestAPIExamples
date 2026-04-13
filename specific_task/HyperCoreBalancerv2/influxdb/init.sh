#!/bin/bash
# =============================================================================
# InfluxDB Initialization Script
# =============================================================================
# Runs during InfluxDB first-time setup on port 9999.
# Creates the long-term bucket and downsampling tasks.
#
# NOTE: The primary 'metrics' bucket doesn't exist yet at this stage —
# it gets created AFTER init scripts complete. Its retention is set
# by the influxdb-init sidecar container which waits for full startup.
# =============================================================================

INIT_PORT="${INFLUXD_INIT_PORT:-9999}"
INFLUX_HOST="http://localhost:${INIT_PORT}"
TOKEN="${DOCKER_INFLUXDB_INIT_ADMIN_TOKEN}"
ORG="${DOCKER_INFLUXDB_INIT_ORG}"
PRIMARY="${DOCKER_INFLUXDB_INIT_BUCKET}"
LONGTERM="${INFLUX_LONGTERM_BUCKET:-metrics_longterm}"
LONGTERM_RET="${INFLUX_LONGTERM_RETENTION:-1825}"
LONGTERM_RET_H=$(( LONGTERM_RET * 24 ))

echo "[INIT] Starting configuration (init port: ${INIT_PORT})..."

# ─── 1. Create long-term bucket ───
echo "[INIT] Creating long-term bucket '${LONGTERM}'..."
influx bucket create \
    --host "$INFLUX_HOST" \
    -t "$TOKEN" \
    -o "$ORG" \
    -n "$LONGTERM" \
    --retention "${LONGTERM_RET_H}h" 2>&1 || \
echo "[INIT] Long-term bucket already exists."

# ─── 2. Create downsampling tasks ───
echo "[INIT] Creating downsampling tasks..."
TMPDIR=$(mktemp -d)

# --- Boolean fields per measurement (these get downsampled with last()) ---
declare -A BOOL_FIELDS
BOOL_FIELDS[node_metrics]="network_online|virtualization_online|allow_running_vms"
BOOL_FIELDS[drive_metrics]="is_healthy"
# vm_metrics and vsd_metrics have no boolean fields — pure numeric
BOOL_FIELDS[vm_metrics]=""
BOOL_FIELDS[vsd_metrics]=""

for M in node_metrics vm_metrics vsd_metrics drive_metrics; do
    BOOLS="${BOOL_FIELDS[$M]}"

    if [ -z "$BOOLS" ]; then
        # No booleans — simple mean over everything
        cat > "${TMPDIR}/${M}.flux" <<FLUX
option task = {name: "downsample_${M}_hourly", every: 1h}

from(bucket: "${PRIMARY}")
    |> range(start: -2h, stop: -1h)
    |> filter(fn: (r) => r["_measurement"] == "${M}")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "${LONGTERM}", org: "${ORG}")
FLUX
    else
        # Split: mean() for numeric fields, last() for boolean fields
        cat > "${TMPDIR}/${M}.flux" <<FLUX
option task = {name: "downsample_${M}_hourly", every: 1h}

base = from(bucket: "${PRIMARY}")
    |> range(start: -2h, stop: -1h)
    |> filter(fn: (r) => r["_measurement"] == "${M}")

numeric = base
    |> filter(fn: (r) => r["_field"] !~ /^(${BOOLS})$/)
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)

boolean = base
    |> filter(fn: (r) => r["_field"] =~ /^(${BOOLS})$/)
    |> aggregateWindow(every: 1h, fn: last, createEmpty: false)

union(tables: [numeric, boolean])
    |> to(bucket: "${LONGTERM}", org: "${ORG}")
FLUX
    fi

    influx task create --host "$INFLUX_HOST" -t "$TOKEN" -o "$ORG" -f "${TMPDIR}/${M}.flux" 2>&1 && \
    echo "[INIT] Created: downsample_${M}_hourly" || \
    echo "[INIT] Skipped: downsample_${M}_hourly (may already exist)"
done

for M in vm_inventory block_device_metrics; do
    cat > "${TMPDIR}/${M}.flux" <<FLUX
option task = {name: "downsample_${M}_daily", every: 1d}

from(bucket: "${PRIMARY}")
    |> range(start: -2d, stop: -1d)
    |> filter(fn: (r) => r["_measurement"] == "${M}")
    |> aggregateWindow(every: 1d, fn: last, createEmpty: false)
    |> to(bucket: "${LONGTERM}", org: "${ORG}")
FLUX
    influx task create --host "$INFLUX_HOST" -t "$TOKEN" -o "$ORG" -f "${TMPDIR}/${M}.flux" 2>&1 && \
    echo "[INIT] Created: downsample_${M}_daily" || \
    echo "[INIT] Skipped: downsample_${M}_daily (may already exist)"
done

for M in migration_events snapshot_metrics replication_metrics condition_metrics cluster_metrics; do
    cat > "${TMPDIR}/${M}.flux" <<FLUX
option task = {name: "archive_${M}_daily", every: 1d}

from(bucket: "${PRIMARY}")
    |> range(start: -2d, stop: -1d)
    |> filter(fn: (r) => r["_measurement"] == "${M}")
    |> to(bucket: "${LONGTERM}", org: "${ORG}")
FLUX
    influx task create --host "$INFLUX_HOST" -t "$TOKEN" -o "$ORG" -f "${TMPDIR}/${M}.flux" 2>&1 && \
    echo "[INIT] Created: archive_${M}_daily" || \
    echo "[INIT] Skipped: archive_${M}_daily (may already exist)"
done

rm -rf "$TMPDIR"

echo "[INIT] ✓ Init script complete."
echo "[INIT]   Long-term: '${LONGTERM}' (${LONGTERM_RET}d)"
echo "[INIT]   Primary bucket retention will be set by the influxdb-init sidecar."