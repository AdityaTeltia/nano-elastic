# Nano-Elastic

A distributed search database implementation in Go, built from scratch without major dependencies like Lucene.

## Features

- Document storage with schema validation
- Write-Ahead Log (WAL) for durability
- File-based segment storage
- Support for multiple field types (text, keyword, numeric, vector, boolean, date)

## Current Status

**Phase 1 Complete**: Basic document storage, persistence, and WAL implementation.

## Quick Start

```bash
# Run Phase 1 demo
go run ./cmd/demo/phase1
```

## Project Structure

```
nano-elastic/
├── cmd/demo/     # Phase-by-phase demos
├── internal/     # Core implementation
│   ├── types/    # Document and schema types
│   └── storage/  # Storage layer (segments, WAL)
└── pkg/          # Public APIs (future)
```

## Roadmap

- Phase 2: Inverted Index Implementation
- Phase 3: BM25 Scoring Algorithm
- Phase 4: Query Execution Engine
- Phase 5: HNSW Vector Index
- Phase 6: Hybrid Search & Reranking
- Phase 7-10: Distributed Architecture

