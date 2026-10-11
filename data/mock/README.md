# Mock marketing data

This directory holds the small, versioned fixture used to prove correctness. Large
datasets are generated under `data/generated/` and are intentionally ignored by Git.

## Source shapes

| Dataset | Simulated source | Shape | Format |
|---|---|---|---|
| `google_ads` | Google Ads daily export | batch aggregate | CSV gzip |
| `meta_ads` | Meta Ads daily export | batch aggregate | JSONL gzip |
| `tiktok_ads` | TikTok Ads daily export | batch aggregate | CSV gzip |
| `web_events` | web/mobile analytics SDK | event stream | JSONL gzip |
| `orders` | commerce service | event stream | JSONL gzip |
| `email_events` | email service | event stream | JSONL gzip |
| `crm` | CRM daily snapshot | batch snapshot | CSV gzip |
| `master/*` | business reference systems | master data | CSV gzip |
| `quality_cases` | deliberately invalid inputs | quarantined examples | JSONL gzip |
| `ground_truth` | generator oracle | expected campaign totals | JSONL gzip |

Identifiers connect campaigns, click touches, sessions, customers, products, and
orders across sources. Ad-platform files remain realistic daily aggregates instead
of inventing user-level impression logs that those platforms would not normally
export.

## Profiles

| Profile | Days | Customers | Campaigns | Products | Impressions/day | Purpose |
|---|---:|---:|---:|---:|---:|---|
| `fixture` | 2 | 100 | 6 | 50 | 500 | committed correctness tests |
| `smoke` | 3 | 2,000 | 12 | 200 | 10,000 | local integration checks |
| `demo` | 14 | 50,000 | 30 | 1,000 | 150,000 | presentation and application demo |
| `benchmark` | 30 | 250,000 | 60 | 5,000 | performance evaluation |

List the authoritative values at any time with `marketpulse-mock profiles`.

## Reproduce the committed fixture

From the repository root:

```bash
PYTHONPATH=src python3 -m marketpulse_mock generate \
  --profile fixture \
  --seed 360 \
  --output data/mock/fixtures/seed-360 \
  --overwrite

PYTHONPATH=src python3 -m marketpulse_mock validate \
  --input data/mock/fixtures/seed-360
```

The manifest is the run's control record. Validation recalculates every compressed
file checksum and checks schemas, foreign keys, web/order reconciliation, platform
aggregates, ground truth, and anomaly counts.

## Kafka replay boundary

`web_events`, `orders`, and `email_events` are replayable sources. Their partitions
are written in nondecreasing event time, including when many journeys overlap.
MP-02 will read these files and publish them to versioned Kafka topics with
`--speed`, `--events-per-second`, pause/resume, and no-sleep benchmark controls.

CRM snapshots and ad-platform reports are batch sources and will be ingested through
Dagster. The generator does not publish to Kafka itself, which keeps source creation
deterministic and lets ingestion tests replay the exact same bytes at different
speeds.
