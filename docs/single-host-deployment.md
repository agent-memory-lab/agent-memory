# Single-host deployment boundary

## Supported operating model

Use one owning process and one `UnifiedMemory` instance for a writable memory
database and its associated recovery stores, partition catalog and deletion
journal. Route concurrent callers through that owner. Ownership is an operational
requirement, not a cross-process lock enforced by the plugin.

Multiple remote clients may use the same single-owner MCP service. Configure one
application worker and one writable replica. Do not run an embedded writer, CLI
writer, maintenance process or a second MCP writer against those same files.
An in-process asynchronous lock does not coordinate independent instances,
threads or processes. SQLite transactions do not make a multi-step capture,
deletion or recovery workflow atomic across processes.

## Maintenance and restart

- Invoke cleanup, retirement and retry operations through the owning instance.
- Stop admitting new writes and drain in-flight work before closing the owner.
- Ensure the old process has exited before starting a replacement writer.
- Preserve the primary database, recovery partitions, catalog and deletion journal
  together. Do not remove fences or catalogs to clear capacity errors.
- Resume pending deletion before exposing memory; do not bypass the recovery gate.
- Treat restored tool execution state as information for the host. Never replay
  side-effecting tools automatically merely because a recovery state exists.
- Avoid overlapping writers during rolling deployments. Use stop-then-start until
  explicit cross-process ownership and fencing are implemented.

## TLS and gateway boundary

The local acceptance gateway is a test fixture, not a production gateway product.
It terminates TLS, validates a bearer credential, removes caller-supplied identity
headers and supplies a signed host identity. Keep certificate and hostname
verification enabled. Store secrets outside source control.

A production deployment must separately establish trusted proxy configuration,
credential rotation, backend reachability restrictions and authorization policy.
Do not expose an identity-signing backend through an unauthenticated path. Local
loopback acceptance does not certify a production ingress, reverse proxy, mTLS,
revocation handling or certificate renewal.

## Evidence and limits

A Python 3.13 single-process baseline ran for 1,800 seconds with four concurrent
capture producers. It completed 65,676 recovery cycles, retired 16,419 partitions
and reopened the memory owner 820 times within the same process. Peak resident
memory was 62,603,264 bytes; observed file descriptors remained at seven. Final
SQLite quick checks passed. Reported p95 cycle latency was approximately 168 ms
over the most recent retained sample window, not the entire run.

This workload checks bounded recovery lifecycle behavior with regular cleanup.
It does not establish multi-process safety, indefinite retention capacity,
distributed recovery, process-restart durability or saturation throughput.
