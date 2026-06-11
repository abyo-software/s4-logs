# Grafana dashboard for the s4logs gateway

`s4logs-gateway.json` is a Grafana dashboard (schemaVersion 39, Grafana 10+)
over the Prometheus metrics the s4logs gateway exposes on `/metrics`.

## Prerequisites

- A Prometheus that **scrapes the gateway's `/metrics` endpoint**. The
  gateway only exports metrics if the process-global Prometheus recorder was
  installed (the default); without scraping there is no data to show. Example
  scrape config:

  ```yaml
  scrape_configs:
    - job_name: s4logs-gateway
      static_configs:
        - targets: ["gateway-host:8080"]
      metrics_path: /metrics
  ```

- That Prometheus added as a Grafana **Prometheus datasource**. The dashboard
  uses a templated `$datasource` variable (type `prometheus`), so you pick the
  datasource at import time — nothing is hardcoded.

## Import

1. Grafana → **Dashboards → New → Import**.
2. Upload `s4logs-gateway.json` (or paste its contents).
3. When prompted, select your Prometheus datasource for the **Datasource**
   variable.
4. Save.

## Panels and the metrics they use

All metrics are emitted by `s4logs-gateway`; the dashboard invents none.

| Panel | Metric(s) |
| --- | --- |
| Events accepted / sec by action (stacked) | `s4logs_events_total{action}` |
| Chunk flush rate | `s4logs_flush_total` |
| Flush throughput: raw vs compressed bytes/sec | `s4logs_flush_bytes_total{kind=raw\|compressed}` |
| Live compression ratio | `s4logs_flush_bytes_total{kind=raw}` / `{kind=compressed}` |
| CloudWatch passthrough error rate | `s4logs_cw_passthrough_errors_total` |
| Backpressure (503) events / sec | `s4logs_backpressure_total` |
| WAL error counters (cumulative stat) | `s4logs_wal_torn_lines_total`, `s4logs_wal_fsync_errors_total`, `s4logs_wal_dir_fsync_errors_total` |
| WAL throughput: appends & replayed / sec | `s4logs_wal_appends_total`, `s4logs_wal_replayed_events_total` |
| WAL error rate (per-sec) | `s4logs_wal_torn_lines_total`, `s4logs_wal_fsync_errors_total`, `s4logs_wal_dir_fsync_errors_total` |

### Notes

- **Compression ratio** is computed from the byte-rate counters over the
  dashboard's rate window, so it reflects live traffic rather than a
  lifetime average.
- The WAL panels are only populated when the gateway runs with a WAL
  directory configured (`--wal-dir`). Without it those series stay empty.
- `s4logs_wal_dir_fsync_errors_total` should be **zero on local filesystems**
  (ext4/xfs). A non-zero value usually means the WAL directory lives on a
  FUSE/NFS mount that rejects `fsync` on a directory handle — durability of
  the segment dirent (create/delete) is then best-effort on that filesystem.
