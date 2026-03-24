# HyperCore Load Balancer — Issue List

## HIGH

_(none currently)_

## MEDIUM

_(none currently)_

## LOW

1. **Credentials use env-var fallback defaults** — should log a startup warning if defaults are in use
2. **SSL verification disabled by default** — should warn loudly if `SC_VERIFY_SSL=False` at startup
3. **Open CORS policy on dashboard** — only exploitable if dashboard is internet-facing
4. **Line 124 bare `except` swallows task status errors silently** — should log before returning `"UNKNOWN"`
5. **No version pinning in requirements.txt** — unpinned deps can break on library updates
6. **Magic numbers scattered through code** — hardcoded multipliers with no named constants
7. **Emoji characters in production logs** — can break log parsers
8. **Inconsistent logging** — custom numeric levels instead of standard `logging` module
9. **No log rotation configured** — Docker logs can grow unbounded
10. **No SLA/success metrics tracked** — migration duration, downtime, and failure rates aren't recorded

## RESOLVED

- ~~**No graceful shutdown on SIGTERM**~~ — fixed: `_handle_sigterm` registered via `signal.signal` in `HyperCore_balancer.py`
- ~~**Inefficient 90-day InfluxDB query on every predictive run**~~ — fixed: parallel `ThreadPoolExecutor` per VM, background thread, configurable lookback via `SC_PREDICTIVE_LOOKBACK_DAYS`, `SC_PREDICTIVE_MAX_WORKERS`
- ~~**Active migration state not persisted**~~ — crash risk fixed: both `migrate()` calls wrapped with error handling; failed migrations set VM cooldown to prevent repeated attempts
- ~~**No health checks in docker-compose**~~ — fixed: InfluxDB health check added; all services now wait for `service_healthy` before starting; collector ping loop retained as additional safeguard
- ~~**No resource limits in docker-compose**~~ — fixed: hard limits added to all containers with explanatory comments warning against over-provisioning; balancer note links CPU limit to `SC_PREDICTIVE_MAX_WORKERS`
- ~~**Env var naming inconsistency**~~ — all application-specific vars now use `SC_` prefix across all scripts, docker-compose.yaml, and .env; new vars `SC_PREDICTIVE_LOOKBACK_DAYS`, `SC_PREDICTIVE_MAX_WORKERS`, `SC_DASHBOARD_STALE_SECONDS` added
- ~~**Synchronous InfluxDB writes in collector**~~ — not an issue in practice; data volume is ~72GB/year at 500 VMs; synchronous mode also enables the existing retry logic
- ~~**No authentication on the Dashboard API**~~ — fixed: HTTP Basic Auth via `require_auth` decorator on all routes; credentials set via `SC_DASHBOARD_USER` / `SC_DASHBOARD_PASSWORD`; auth self-disables if vars are unset
- ~~**No connection pooling in dashboard**~~ — not an issue in practice; dashboard is a personal tool used on-demand by a single user
- ~~**Dry-run mutates state**~~ — intentional; prevents the same VM being recommended repeatedly during dry-run testing
- ~~**No timeout on migration task polling**~~ — not an issue; cluster manages task timeouts and returns `ERROR` state
- ~~**Bare `except:` clauses on logout/close**~~ — intentional; logout must be attempted even during shutdown
- ~~**Unsafe dictionary/list access**~~ — false positive; guards are in place throughout
- ~~**Hardcoded credentials**~~ — fallback defaults only; `get_config()` always checks env vars first
- ~~**SSL verification disabled**~~ — moved to low; default is for dev use only
- ~~**Open CORS policy**~~ — moved to low; only relevant if internet-facing
