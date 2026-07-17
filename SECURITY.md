# Security policy

## Supported versions

| Version | Supported |
|---------|-----------|
| main (HEAD) | ✓ |
| older tags | ✗ |

geo-redis is pre-1.0 and does not yet make backward-compatibility guarantees between releases.

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email **security@geo-redis-geo.io** (or the repository owner's contact in the GitHub profile)
with the subject line `[geo-redis] Security vulnerability report`. Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce (proof-of-concept code if available)
- Any proposed mitigation

You will receive an acknowledgment within **48 hours** and a status update within **7 days**.

## Known security posture

| Area | Current status |
|------|----------------|
| Write-endpoint auth | Optional API-key (`API_KEY` env). Off by default in dev. Enable in production. |
| Transport encryption | No built-in TLS. Run behind a TLS-terminating proxy or use `rediss://` for the Redis connection. |
| Range-assignment consensus | Redis SET NX CAS lock — not partition-safe. See [TECHNICAL.md §5.3](TECHNICAL.md). |
| CORS | Permissive (`CorsLayer::permissive`) in the demo binary. Restrict in production. |
| gRPC | Plain text. Use a service mesh or mutual TLS for production deployments. |

## Scope

In-scope:
- Remote code execution, privilege escalation, data exfiltration via the REST or gRPC API
- Authentication bypass (API-key check logic)
- Denial-of-service via specially crafted payloads

Out-of-scope:
- Vulnerabilities requiring physical access to the host
- Issues in Redis, Docker, or Kubernetes themselves
- Known limitations documented in TECHNICAL.md (these are design gaps, not security bugs)
