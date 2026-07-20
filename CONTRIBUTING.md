# Contributing

Use Node.js 24 and pnpm 11 for local development. Compatibility is also verified on supported Node.js
22 and current Node.js 26.

```bash
pnpm install --frozen-lockfile
pnpm verify
```

Changes should preserve these rules:

- Keep the runtime implementation independent from HTTP frameworks.
- Do not replace bounded queues with unbounded promise creation.
- Make resource ownership and shutdown behavior explicit.
- Treat cancellation as cooperative unless an entire worker is terminated.
- Add deterministic behavioral tests for race and failure paths.
- Keep performance output observational and out of correctness assertions.
- Do not add customer events, access tokens, or private infrastructure data to fixtures.

Commit messages should be short and imperative. Pull requests should describe the failure mode being
addressed, the chosen guarantee, its boundary, and the commands used for verification.
