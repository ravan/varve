# Operations guide

This section is for running Varve in production, not for writing GQL against it. Start with
[Deployment profiles & sizing](profiles.md) to pick a topology and the handful of tuning knobs
that actually matter at your scale; consult the [configuration reference](configuration.md) for
every `[section]` key `varve.toml` accepts; read [Failover](failover.md) before choosing between
the default `designated-writer` mode and the opt-in, probe-gated `cas-failover` mode; and wire
[Metrics & observability](metrics.md) into Prometheus/Grafana and, optionally, an OpenTelemetry
collector before you need them at 2am. [Backends & capability matrix](../backends.md) covers
which S3-API object stores are CI-verified and how to configure each.
