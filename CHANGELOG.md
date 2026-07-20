# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-20

### Added

- Bounded worker pool with cooperative cancellation, crash replacement, and diagnostics events
- Backpressure-aware NDJSON decoder and ordered concurrent mapping
- AsyncLocalStorage request context with explicit worker-boundary propagation
- Node core ingestion API with health, readiness, metrics, and overload responses
- Phased graceful shutdown with real process-signal verification
- Runtime scenarios, coverage thresholds, multi-version CI, and release automation

[Unreleased]: https://github.com/wasiliy-strecker/node-reliability-lab/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/wasiliy-strecker/node-reliability-lab/releases/tag/v0.1.0
