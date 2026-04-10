# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [2.2.9] - 2026-04-09
### Added
- Separate NuGet-specific README (NUGET_README.md) without relative images or Mermaid diagrams

### Changed
- Package now uses NUGET_README.md for NuGet display instead of the GitHub README

### Removed
- .claude/skills git submodule from tracking (now ignored via .gitignore)

### Added
- CLAUDE.md project instructions for Claude Code sessions
- Animated author banner SVG (.github/banner.svg) — Ocean Depths style
- Rich README.md with class diagrams, sequence diagrams, collapsible service coverage, full CI/CD pipeline docs
- Comprehensive .gitignore with secrets, IDE, Claude/AI, OS, logs, test, and database sections

## [2.2.6] - 2026-04-09

### Added

- Initial C# port of aws-util Python package (v2.2.6)
- Core infrastructure: ClientFactory with TTL-based caching, structured exception hierarchy, error classifier
- Placeholder resolution for SSM Parameter Store and Secrets Manager references
- Config loader for batch application configuration from SSM + Secrets Manager
- 100+ AWS service wrappers covering compute, storage, database, networking, AI/ML, security, and more
- Multi-service orchestration modules: deployer, data pipeline, security ops, resilience, observability, etc.
- All operations are async (Task-based) with proper error classification
- Unit test project with xUnit
