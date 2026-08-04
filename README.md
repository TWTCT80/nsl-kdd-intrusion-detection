# NSL-KDD Intrusion Detection Pipeline

ML-pipeline för nätverksintrångsdetektering med NSL-KDD-datasetet.

## Arkitektur

```
KDDTrain+.txt
     │
     ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
│  Snowflake  │────▶│     dbt     │────▶│   Databricks    │
│  (rådata)   │     │ (features)  │     │ (Random Forest) │
└─────────────┘     └─────────────┘     └────────┬────────┘
                                                  │
                                    ┌─────────────┴──────────────┐
                                    │                            │
                               ┌────▼─────┐              ┌──────▼──────┐
                               │  MLflow  │              │    Slack    │
                               │(tracking)│              │  (rapport)  │
                               └──────────┘              └─────────────┘
```

Varje lager har ett tydligt ansvar: Snowflake lagrar rådata, dbt håller
transformationslogiken versionshanterad och återanvändbar, Databricks tränar och
utvärderar modellerna, MLflow spårar alla körningar, och Slack levererar resultatet
utan att någon behöver logga in manuellt.

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

**A/B-test:** v1 och v2 jämfördes med 70/30 trafikdelning. v2 presterar +0.0018 bättre
i F1 men kräver dubbelt så många träd. Beslutsregeln kräver minst 0.01 förbättring för
att motivera den ökade resurskostnaden — **v1 behålls i produktion**.

## Dataset
NSL-KDD (KDDTrain+) — 125 973 nätverksanslutningar med 5 attackkategorier:
normal, DoS, Probe, R2L, U2R

Utvärderingen sker på en 80/20-split av KDDTrain+ (100 778 / 25 195 rader).

## Köra projektet

Notebooken kan inte köras rakt igenom med "Run all" — två celler bryter flödet:

1. Cell 1 — `%pip install`, vänta tills den är klar
2. Cell 2 — `%restart_python`, vänta tills kerneln är uppe igen
3. Cell 3 — anslutning till Snowflake, kräver lösenord via `getpass()`
4. Cell 4–11 — kan köras i ett svep

Snowflakes compute-motor (`COMPUTE_WH`) måste vara startad (Resume) innan cell 3.

## Struktur

```
├── dbt_models/models/      dbt-transformationer (sources, staging, fct)
├── notebooks/              Databricks-notebook med hela ML-pipelinen
├── models/                 Tränad modell + scaler (.pkl, via Git LFS)
├── data/                   FCT_FEATURES-export (.csv, via Git LFS)
├── screenshots/            Slack-rapport, dashboard, MLflow-experiment
└── docs/                   Fullständig writeup
```

## Projektdokumentation
Se `docs/MLPipeline_NSL_KDD_Writeup.md` för fullständig writeup.

## OBS
Webhook-URL och lösenord finns INTE i koden — ersatta med platshållare.
Snowflake-lösenordet anges via `getpass()` vid körning och sparas aldrig i filen.
