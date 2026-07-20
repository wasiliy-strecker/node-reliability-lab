# Security policy

## Supported versions

Security fixes are applied to the latest release on `main`.

## Reporting a vulnerability

Please use GitHub private vulnerability reporting when it is available for this repository. Do not
open a public issue containing an exploit, secret, private payload, or infrastructure detail.

Include the affected revision, reproduction steps, expected impact, and whether the issue can cross
a process or network trust boundary. Sanitized fixtures are preferred.

## Security boundaries

- The service accepts untrusted NDJSON only within explicit body, line, record, request, worker, and
  queue limits.
- Caller-provided request IDs are accepted only from a narrow character set and length.
- Problem responses never include worker stacks or internal exception text for unknown failures.
- The sample service has no authentication. It must not be exposed as a production endpoint without
  an application-specific identity and authorization layer.
- `AsyncLocalStorage` contains correlation metadata and must not be treated as an authorization
  source.
- Fingerprinting is a deterministic workload demonstration, not a password hashing or signature
  scheme.
- No input event or payload is written to logs by the provided logger.

Dependency updates are reviewed through Dependabot. Routine minor and patch upgrades are grouped,
while breaking major migrations are planned explicitly. Security updates remain enabled.
