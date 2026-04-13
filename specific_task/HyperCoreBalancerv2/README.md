# HyperCore Cluster Balancer

**Reactive, Predictive & Affinity-Based Load Balancing for Scale Computing HyperCore**

The HyperCore Cluster Balancer is a containerized solution that automatically distributes virtual machine workloads across Scale Computing HyperCore cluster nodes. It monitors resource usage in real time, enforces VM placement rules, and — optionally — uses machine learning to predict and prevent future overloads before they happen.

---

## Features

- **Reactive Balancing** — Continuously monitors node CPU and RAM usage via sliding-window averages. When a node exceeds configurable thresholds, the lightest suitable VM is live-migrated to the least-loaded node.
- **Predictive Balancing (The Oracle)** — Uses [Facebook Prophet](https://facebook.github.io/prophet/) to forecast per-VM CPU load 24 hours ahead. Proactively migrates VMs before predicted spikes arrive. Runs in a background thread with parallel workers so the reactive engine is never blocked.
- **Affinity Enforcement** — Highest-priority engine that enforces VM placement rules via HyperCore tags:
  - `node_<suffix>` — Pin a VM to a specific node (e.g. `node_241` pins to the node whose IP ends in `.241`)
  - `anti_<vm_name>` — Prevent two VMs from sharing the same node (e.g. `anti_dc02` on `dc01`)
- **Live Configuration** — All tunable settings are stored in a shared SQLite database. Change any parameter through the dashboard UI at runtime — no container restarts required.
- **Built-in Dashboard** — Web-based UI (port 5000) showing node/VM performance, drive health, VSD I/O, migration event log, and a live settings editor. Fully offline-capable for dark-site deployments.
- **Metrics Collection** — Comprehensive HyperCore telemetry (node performance, VM stats, drive health, snapshots, replication, cluster conditions) stored in InfluxDB with automatic downsampling.
- **Grafana Integration** — Optional advanced dashboarding via Grafana, available on-demand through Docker profiles.

## Architecture

```
┌────────────┐     ┌────────────┐      ┌─────────────┐
│  Collector │────▶│  InfluxDB  │◀────│  Dashboard  │
│  (30s poll)│     │ (metrics)  │      │  (port 5000)│
└────────────┘     └─────┬──────┘      └─────────────┘
                         │
                   ┌─────▼───────┐
                   │  Balancer   │──────▶ HyperCore REST API
                   │ (reactive + │       (live migrations)
                   │  predictive │
                   │  + affinity)│
                   └─────────────┘
```

| Container   | Role                                                        |
|-------------|-------------------------------------------------------------|
| **Collector** | Polls HyperCore API, writes metrics to InfluxDB            |
| **Balancer**  | Decision engine — affinity, reactive, and predictive modes |
| **InfluxDB**  | Time-series storage with 1-year detailed + 5-year long-term retention |
| **Dashboard** | Web UI for monitoring and live configuration               |
| **Grafana**   | *(optional)* Advanced data exploration                     |

## Quick Start

```bash
git clone <repository-url> HyperCoreBalancer
cd HyperCoreBalancer

cp .env.template .env
# Edit .env — set SC_HOST, SC_USERNAME, SC_PASSWORD, and change all CHANGE_ME values

docker compose up --build -d
```

The dashboard is available at **http://localhost:5000**.

> **Note:** On first startup, the balancer needs ~2.5 minutes to fill its averaging buffers before it begins evaluating the cluster. The dashboard will show data after the first collector cycle (~30 seconds).

### Reactive-Only Mode (No InfluxDB / Dashboard)

If you only need reactive load balancing without metrics storage or the web UI:

```bash
# Create a minimal .env with only cluster credentials and reactive settings
# Set SC_PREDICTIVE_BALANCING_ENABLED=false

docker compose up --build -d balancer
```

See the [manual](HyperCore_Cluster_Balancer_Manual_v2.docx) section 1.2.1 for the full minimal `.env` template.

## Configuration

All settings are defined in `.env` and seeded into a shared SQLite config database on first startup. After that, settings can be changed live through the dashboard UI.

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SC_DRY_RUN` | `true` | **Start here.** Logs decisions without executing migrations. |
| `SC_CPU_UPPER_THRESHOLD_PERCENT` | `50.0` | Node CPU % that triggers reactive balancing |
| `SC_RAM_UPPER_THRESHOLD_PERCENT` | `65.0` | Node RAM % that triggers reactive balancing |
| `SC_RAM_LIMIT_PERCENT` | `85.0` | Hard ceiling — never overload a target node beyond this |
| `SC_MAX_VCPU_RATIO` | `2.0` | Max vCPU-to-thread overcommit ratio |
| `SC_PREDICTIVE_BALANCING_ENABLED` | `true` | Enable/disable the Oracle |
| `SC_PREDICTIVE_INTERVAL_SECONDS` | `43200` | How often the Oracle runs (default: 12 hours) |
| `SC_PREDICTIVE_MAX_WORKERS` | `8` | Parallel threads for VM forecasting |
| `SC_EXCLUDE_NODE_IPS` | *(empty)* | Comma-separated node IPs to exclude from balancing |

See the [full configuration reference](HyperCore_Cluster_Balancer_Manual_v2.docx) in Part 2 of the manual.

## VM Placement Tags

Placement rules are defined as tags on VMs in HyperCore — no balancer configuration needed.

| Tag Format | Effect | Example |
|------------|--------|---------|
| `node_<suffix>` | Pin VM to the node whose LAN IP ends with `<suffix>` | `node_241` |
| `anti_<vm_name>` | Never place this VM on the same node as `<vm_name>` | `anti_dc02` |

## Dark-Site / Air-Gapped Deployment

```bash
# On an internet-connected machine:
docker compose build
docker save $(docker compose config --images) -o hypercore-balancer-images.tar

# On the air-gapped machine:
docker load -i hypercore-balancer-images.tar
docker compose up -d
```

The entire stack runs fully offline once images are built.

## Resource Requirements

| VMs  | Min. vCPUs | Recommended RAM | Disk (1yr) |
|------|-----------|-----------------|------------|
| 5    | 2         | 1 GB            | 2 GB       |
| 50   | 2         | 1 GB            | 12 GB      |
| 100  | 2         | 2 GB            | 23 GB      |
| 500  | 4         | 4 GB            | 112 GB     |

## Grafana

Grafana is not started by default. To enable it:

```bash
docker compose --profile tools up -d grafana
# Available at http://localhost:3000
```

Connect it to InfluxDB using Flux query language at `http://influxdb:8086`.

## Project Structure

```
├── docker-compose.yaml
├── .env.template
├── .gitignore
├── influxdb/
│   ├── Dockerfile
│   └── init.sh              # Creates long-term bucket & downsampling tasks
├── Collector/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config_db.py          # Shared live-config module
│   └── collector.py
├── Balancer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config_db.py
│   ├── HyperCore_balancer.py  # Main decision engine
│   └── predictive_engine.py   # The Oracle (Prophet forecasting)
└── dashboard/
    ├── Dockerfile
    ├── requirements.txt
    ├── config_db.py
    ├── app.py                 # Flask API
    └── static/
        └── index.html         # Single-page dashboard
```

## Documentation

The complete installation, configuration, and operations manual is available as a Word document:

[HyperCore Cluster Balancer Manual v2.0](HyperCore_Cluster_Balancer_Manual_v2.docx)

---

## Disclaimer

**USE AT YOUR OWN RISK.** While the HyperCore Cluster Balancer has been carefully developed and tested, it is provided "as is" without warranty of any kind, express or implied. Scale Computing accepts no responsibility or liability for any damage, data loss, downtime, or other consequences resulting from the use of this software. Always run in dry-run mode (`SC_DRY_RUN=true`) for an extended observation period before enabling live migrations.

## License

MIT License

Copyright (c) 2026 Scale Computing, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
