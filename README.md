# NSL-KDD Intrusion Detection Pipeline

ML-pipeline för nätverksintrångsdetektering med NSL-KDD-datasetet.

## Arkitektur
## Verktyg
- **Snowflake** — Data warehouse (NSL_KDD_DB)
- **dbt Cloud** — Feature engineering och transformationer
- **Databricks** — ML-modellträning (Random Forest, Isolation Forest)
- **MLflow** — Experiment tracking
- **Slack** — Automatiska modellrapporter

## Resultat
| Modell | F1-score |
|---|---|
| Isolation Forest | 0.642 |
| Random Forest v1 | 0.995 |
| Random Forest v2 | 0.997 |

## Dataset
NSL-KDD (KDDTrain+) — 125 973 nätverksanslutningar med 5 attackkategorier:
normal, DoS, Probe, R2L, U2R

## Projektdokumentation
Se `docs/MLPipeline_NSL_KDD_Writeup.md` för fullständig writeup.

## OBS
Webhook-URL och lösenord finns INTE i koden — ersatta med platshållare.
