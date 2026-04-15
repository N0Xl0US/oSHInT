# TEMPORAL IDENTITY PERSONA BUILDER — Engineering Handoff
Phase 2 & Phase 3 — Receiver's Brief

---

## Metadata

| Attribute | Detail | Notes |
|----------|--------|------|
| Status | Phase 1 In Progress | Use mock golden records |
| Version | v0.1 — Draft Handoff | Update after Phase 1 |
| Owner | Phase 1 Team | Contact for schema changes |
| Consumers | Phase 2, Phase 3 Teams | Primary audience |
| Stack Constraint | 100% FOSS | No commercial tools |
| Privacy | GDPR / OPIF compliant | Data minimization mandatory |
| Graph DB | Neo4j Community Edition | Single-node only |

---

## ⚠️ Read First

Phase 1 is still under development.  
Do NOT block on Phase 1. Use mock fixtures until Kafka emits real golden records.

---

# 0. Purpose

This document enables Phase 2 (EvoKG) and Phase 3 (Persona Synthesis) to proceed independently.

Defines:
- Golden Record schema
- Kafka interface
- Neo4j temporal graph
- Persona synthesis system
- Shared rules and thresholds
- Mock fixtures

---

# 1. Golden Record

## Definition

A Golden Record is a deduplicated, pseudonymized representation of an individual built from OSINT signals.

---

## Schema

```json
{
  "entity_id": "uuid-v4",
  "schema_version": "1.0",
  "resolved_at": "ISO-8601 UTC",
  "confidence_score": 0.0,
  "resolution_method": "deterministic | probabilistic | hybrid",

  "aliases": [
    {
      "type": "email | username | phone | handle",
      "value": "hmac_sha256",
      "platform": "string | null",
      "confidence": 0.0,
      "source": "string"
    }
  ],

  "attributes": {
    "name_variants": ["string"],
    "locations": ["string"],
    "languages": ["string"],
    "institutions": ["string"],
    "research_domains": ["string"],
    "co_authors": ["entity_id | null"]
  },

  "sources": [
    {
      "tool": "string",
      "ingested_at": "ISO-8601 UTC",
      "reliability_weight": 0.0
    }
  ],

  "merge_history": [
    {
      "fragment_id": "uuid-v4",
      "merged_at": "ISO-8601 UTC",
      "merge_score": 0.0
    }
  ],

  "quality_flags": {
    "low_confidence": false,
    "conflicting_data": false,
    "quarantined": false,
    "needs_review": false
  },

  "data_governance": {
    "retention_expires_at": "ISO-8601 UTC",
    "legal_basis": "string",
    "erasure_requested": false,
    "rectification_pending": false
  }
}
```

---

## Confidence Thresholds

| Score | Action |
|------|--------|
| 0.85–1.00 | Direct ingestion |
| 0.60–0.84 | Ingest with flags |
| 0.40–0.59 | Quarantine |
| 0.00–0.39 | Block |

---

## Mock Fixtures

### High Confidence

```json
{
  "entity_id": "mock-high",
  "confidence_score": 0.91,
  "quality_flags": {
    "quarantined": false
  }
}
```

### Conflicting Data

```json
{
  "entity_id": "mock-conflict",
  "confidence_score": 0.71,
  "quality_flags": {
    "low_confidence": true,
    "conflicting_data": true,
    "needs_review": true,
    "quarantined": false
  }
}
```

### Quarantined

```json
{
  "entity_id": "mock-quarantine",
  "confidence_score": 0.31,
  "quality_flags": {
    "quarantined": true
  }
}
```

---

# 2. Kafka

## Topics

| Topic | Purpose |
|------|--------|
| osint.raw.ingest | Raw OSINT |
| osint.normalized | Preprocessing |
| identity.resolved | Golden Records |
| identity.quarantine | Low confidence |
| identity.conflicts | Conflict handling |
| persona.reports | Final output |
| audit.events | Audit logs |

---

## Message Format

```json
{
  "envelope": {
    "message_id": "uuid-v4",
    "topic": "identity.resolved",
    "schema_version": "1.0",
    "emitted_at": "ISO-8601 UTC",
    "source_phase": "phase_1"
  },
  "payload": {}
}
```

---

## Partitioning

- Key: entity_id  
- Guarantees ordering per entity

---

## Consumer Groups

| Group | Role |
|------|------|
| cg.phase2.evokg.ingestor | Graph ingestion |
| cg.phase2.contradiction.resolver | Conflict resolution |
| cg.phase3.persona.synthesizer | Persona generation |
| cg.phase3.drift.detector | Drift detection |

---

# 3. EvoKG

## Objectives

- Build temporal graph
- Resolve contradictions
- Support time queries
- Emit graph context

---

## Node Types

- Person
- Alias
- Organization
- Skill
- Location
- ResearchPaper
- TemporalInterval

---

## Relationship Example

```cypher
(Person)-[:WORKED_AT {
  valid_from,
  valid_to,
  role_title,
  confidence,
  source,
  source_reliability,
  conflict_flag
}]->(Organization)
```

---

## Conflict Resolution

| Type | Action |
|------|--------|
| Employment | Highest confidence |
| Skills | Keep all |
| Date conflicts | Use reliability |
| Names | Keep all |

---

## Algorithm

```python
if new_score > existing_score + 0.15:
    replace()
elif abs(new_score - existing_score) <= 0.15:
    send_to_HITL()
else:
    discard()
```

---

# 4. Persona Synthesis

## Objectives

- Generate reports
- Detect drift
- Enforce HITL

---

## Stack

- LLaMA / Mistral
- LangGraph
- LangChain
- sentence-transformers

---

## Drift Detection

```python
distance = 1 - cosine_similarity

if distance > 0.55:
    ALERT
elif distance > 0.35:
    DRIFT
else:
    STABLE
```

---

## Output Schema

```json
{
  "report_id": "uuid-v4",
  "entity_id": "uuid-v4",
  "generated_at": "ISO-8601 UTC",
  "career_summary": "string",
  "risk_flags": [],
  "confidence_assessment": {
    "overall": 0.0
  }
}
```

---

# 5. Academic Data

## Sources

- Semantic Scholar
- OpenAlex
- arXiv
- ORCID
- CrossRef

---

## Disambiguation

1. ORCID match (0.97)
2. Email + institution (0.85)
3. Co-author overlap (0.78)
4. Domain similarity (0.65)

---

# 6. Compliance

## GDPR Rules

- Pseudonymize all PII
- No raw storage
- Erasure within 72 hours
- Audit logging mandatory

---

## HITL Triggers

| Condition | Action |
|----------|--------|
| needs_review | Manual review |
| low confidence | Flag |
| drift alert | Escalate |

---

# 7. Conventions

## Dates

- ISO-8601
- YYYY-MM
- YYYY

## Null Handling

- Use null
- Never empty string
- Never infer

---

## Version Handling

```python
if schema_version not supported:
    send_to_dlq()
```

---

## DLQ Topics

- dlq.phase2.ingest_failures
- dlq.phase3.synthesis_failures
- dlq.schema_version_unsupported

---

## Services

- kafka
- zookeeper
- neo4j
- maigret
- evokg-ingestor
- persona-synthesizer
- ollama
- grafana

---

# End of Document