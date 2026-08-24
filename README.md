# Media Mirror Sync

Automated scheduled data pipeline. Reads indexed entries from the database, prepares secondary renditions, publishes them to external storage, and records the results back.

## Triggers
- `workflow_dispatch` (optional `limit` input)
- `repository_dispatch` type `trigger-mirror-sync` (optional `client_payload.limit`)

## Required Secrets
| Secret | Description |
|---|---|
| `DB_URL` | Database endpoint URL |
| `DB_TOKEN` | Database auth token |
| `STORAGE_KEY` | External storage API key |
| `RELAY_URL` | Relay service endpoint for fetching job payloads |

## Optional Env
| Name | Default | Description |
|---|---|---|
| `MAX_MIRRORS_PER_RUN` | `2` | Max entries processed per run |
| `DOWNLOAD_TIMEOUT` | `360` | Per-item fetch timeout (seconds) |
| `MIN_SEEDERS` | `7` | Availability threshold for non-trusted sources |
| `MISSING_GRACE_SECONDS` | `172800` | Grace period before marking a rendition unavailable |
| `MAX_SEARCH_QUERIES` | `12` | Upper bound of search queries per item |

## Local Run
```bash
pip install -r requirements.txt
export DB_URL=... DB_TOKEN=... STORAGE_KEY=...
python mirror_job.py
```
