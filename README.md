# Hub Flywheel

Monitoring dashboard for P3rP and Chad trading pipelines. Visualizes:
- **Flywheel**: 6-stage trading pipeline (SENSE → SCORE → LEARN → SCREEN → JUDGE → HEAL)
- **Agent Tree**: P3rP + Chad bot network topology
- **Fleet Status**: Active bots, farming status
- **Pipeline Metrics**: Signals generated, win rates, PnL

## Structure

- `html/` - Frontend (compiled from P3rP dashboard)
- `scripts/` - Backend data collectors
  - `flywheel_status.py` - Monitors all pipeline services

## Setup

```bash
# Generate status data
python scripts/flywheel_status.py > html/flywheel.json
python scripts/hub_api.py  # Generate other data files
```

## Deployment

```bash
git push origin main
./deploy.sh  # Copies html/ to /var/www/hub/
```

## Data Files Generated

- `flywheel.json` - Full pipeline status (all services)
- `status.json` - Quick status snapshot (bots, brain state)
- `chad_metrics.json` - Chad bot metrics

