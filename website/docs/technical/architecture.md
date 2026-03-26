---
sidebar_position: 1
title: Architecture
---

# Architecture

Genie World is organized into four modular blocks, each with a clear responsibility and composable API.

## Block Diagram

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│   Profiler   │────▶│   Builder   │────▶│  Benchmarks  │
│              │     │             │     │              │
│ Scan schema, │     │ Generate    │     │ Run, evaluate│
│ detect rels, │     │ config,     │     │ diagnose,    │
│ enrich descs │     │ deploy      │     │ suggest,     │
│              │     │             │     │ update       │
└─────────────┘     └─────────────┘     └──────────────┘
       │                   │                    │
       └───────────────────┴────────────────────┘
                           │
                    ┌──────┴──────┐
                    │    Core     │
                    │             │
                    │ Auth, SQL,  │
                    │ LLM, Config,│
                    │ Tracing     │
                    └─────────────┘
```

## Data Flow

1. **Profiler** reads Unity Catalog metadata and optionally runs SQL queries to produce a `SchemaProfile`
2. **Builder** consumes the profile and generates a complete Genie Space config (`BuildResult`)
3. **Builder** deploys the config via the Databricks API
4. **Benchmarks** runs questions against the live space, evaluates results, and iteratively improves the config

## Design Principles

- **Library-first** — pip-installable, no CLI or webapp required
- **Composable** — each block works independently or together
- **Tiered profiling** — metadata-only (no warehouse) through deep SQL + LLM enrichment
- **Transparent filtering** — table suggestions are recommendations, not silent auto-filters
- **Validation** — SQL examples are validated against the warehouse before inclusion
