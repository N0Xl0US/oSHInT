# oSHint

AI-enabled OSINT pipeline with two execution surfaces:

- FastAPI service for HTTP clients and browser UI
- MCP stdio server for tool-based agent workflows

Core flow:

1. Collect claims from one or more sources (Maigret, Blackbird, Holehe, SpiderFoot, GitHub, Academic, LinkedIn)
2. Score/filter raw signals into normalized identity claims
3. Resolve claims into clustered entities (Splink-based linker)
4. Build golden records
5. Publish results/events to Kafka topics

## Repository Layout

- `maigret/`: main application package (API, MCP server, orchestrator, resolver, source adapters)
- `frontend/`: browser UI served from `/`
- `documents/`: handover and project notes
- `scripts/`: helper scripts (example: SpiderFoot wrapper)

## Prerequisites

- Python 3.12+
- Pip and virtualenv
- Kafka broker reachable from your machine (for publish/readiness behavior)
- Optional source binaries/tools configured in environment (Holehe, Blackbird, SpiderFoot)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

LinkedIn scraping uses Playwright via `linkedin_scraper`; install browser runtime once:

```bash
.venv/bin/playwright install chromium
```

## Environment Configuration

Configuration is loaded from `.env` (see settings usage in `maigret/config.py` and `maigret/orchestrator/config.py`).

Typical local variables:

```env
# API
SERVER_HOST=0.0.0.0
SERVER_PORT=8000

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=osint.raw.maigret.v1

# Source binaries (set to real paths in your environment)
HOLEHE_BINARY=/absolute/path/to/holehe
BLACKBIRD_BINARY=/absolute/path/to/blackbird.py
BLACKBIRD_ROOT=/absolute/path/to/blackbird
SPIDERFOOT_BINARY=/absolute/path/to/spiderfoot-wrapper.sh

# LinkedIn
LINKEDIN_LI_AT=<fresh_li_at_cookie>
LINKEDIN_SESSION_PATH=./linkedin_session.json
LINKEDIN_DAILY_LIMIT=50
LINKEDIN_SCRAPE_TIMEOUT=15
```

Notes:

- Keep real credentials only in local `.env` (never commit secrets).
- LinkedIn adapter prefers `LINKEDIN_LI_AT` when present.

## Running the Services

Start FastAPI server:

```bash
.venv/bin/python -m maigret.main
```

Start MCP stdio server:

```bash
.venv/bin/python -m maigret.mcp_server
```

Health checks:

```bash
curl -sS http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/ready
```

UI:

- Open `http://127.0.0.1:8000/`

## Main API Endpoints

- `GET /health` - liveness
- `GET /ready` - readiness (Kafka connectivity)
- `GET /search_username` - raw Maigret profile discovery
- `GET /search_username_filtered` - Maigret + scoring/filter pipeline
- `GET /search_academic` - academic source lookup
- `GET /search_github` - GitHub source lookup
- `GET /resolve_identity` - orchestrated fan-out + entity resolution
- `POST /resolve_identity_intake` - resume-driven intake then identity resolution

## How the Project Works

### 1. Intake and Routing

The system accepts direct hints (username, email, name, LinkedIn URL) or resume uploads.
Routing logic selects relevant sources per input type.

### 2. Source Fan-Out

Orchestrator launches enabled sources concurrently with per-source timeouts.
Each source emits normalized `IdentityClaim` objects.

### 3. Claim Normalization and Scoring

Claims include platform, URL, confidence/tier, and source-specific raw payloads.
Low-confidence or incomplete claims can be marked for review.

### 4. Entity Resolution

All claims are linked/clustered using the resolver layer (Splink-backed pipeline).
Clusters are transformed into golden records.

### 5. Event Publishing

Raw/source events and resolved identity events are emitted to Kafka topics through the producer layer.

## MCP Mode

`maigret/mcp_server.py` exposes tool functions over MCP stdio for agent execution.
Use this mode when integrating with LLM tool-calling workflows.

## Troubleshooting

- LinkedIn claims are zero with auth error:
  - refresh `LINKEDIN_LI_AT` from a currently logged-in browser session
  - restart API/MCP so new env values are loaded
- LinkedIn scraper startup errors mentioning missing browser executable:
  - run `.venv/bin/playwright install chromium`
- `ready` fails while `health` is OK:
  - verify Kafka broker address and reachability
