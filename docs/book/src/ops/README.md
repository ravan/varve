# Operations guide

This section covers running Varve in production rather than writing GQL against it. Start with
[Deployment profiles & sizing](profiles.md) to pick a topology and the tuning knobs that matter
at your scale. Consult the [configuration reference](configuration.md) for every `[section]`
key `varve.toml` accepts. Read [Failover](failover.md) before choosing between the default
`designated-writer` mode and the opt-in, probe-gated `cas-failover` mode. Wire
[Metrics & observability](metrics.md) into Prometheus/Grafana, and optionally an OpenTelemetry
collector, before you need it. [Backends & capability matrix](../backends.md) covers which
S3-API object stores are CI-verified and how to configure each.
