# ML-pipeline för nätverksintrångsdetektering
### NSL-KDD | Snowflake · dbt · Databricks · MLflow · Slack

**Kurs:** AI, Automation och Machine Learning  
**Uppgift:** Projektuppgift 2 – Avancerad  
**Verktyg:** Snowflake · dbt Cloud · Databricks Community Edition · MLflow · Slack · GitHub  
**Dataset:** NSL-KDD (KDDTrain+) — 125 973 nätverksanslutningar

---

## Innehållsförteckning

1. [Projektöversikt](#1-projektöversikt)
2. [Dataset – NSL-KDD](#2-dataset--nsl-kdd)
3. [Snowflake – Data Warehouse](#3-snowflake--data-warehouse)
4. [dbt – Transformationer](#4-dbt--transformationer) *(kommer)*
5. [Databricks – ML-modell](#5-databricks--ml-modell) *(kommer)*
6. [MLflow – Experiment tracking](#6-mlflow--experiment-tracking) *(kommer)*
7. [Automatisering – Slack-rapport](#7-automatisering--slack-rapport) *(kommer)*
8. [Dashboard – Databricks SQL](#8-dashboard--databricks-sql) *(kommer)*
9. [Resultat och analys](#9-resultat-och-analys) *(kommer)*
10. [GitHub](#10-github) *(kommer)*

---

## 1. Projektöversikt

Det här projektet bygger en komplett maskininlärningspipeline för att detektera nätverksintrång i realtidsdata. Arkitekturen följer ett modernt produktionsupplägg med separata lager för lagring, transformering och ML.

### Arkitektur

```
KDDTrain+.txt
     │
     ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
│  Snowflake  │────▶│     dbt     │────▶│   Databricks    │
│ (rådata)    │     │(features)   │     │ (Random Forest) │
└─────────────┘     └─────────────┘     └────────┬────────┘
                                                  │
                                    ┌─────────────┴──────────────┐
                                    │                            │
                               ┌────▼─────┐              ┌──────▼──────┐
                               │  MLflow  │              │    Slack    │
                               │(tracking)│              │  (rapport)  │
                               └──────────┘              └─────────────┘
```

### Varför denna arkitektur?

| Lager | Verktyg | Syfte |
|---|---|---|
| Data warehouse | Snowflake | Skalbar lagring, alltid tillgänglig för hela teamet |
| Transformering | dbt | Återanvändbar SQL-logik, versionshanterad feature engineering |
| ML-plattform | Databricks | Hanterar stora datamängder, inbyggd MLflow |
| Experiment tracking | MLflow | Spårar alla modellversioner och metrics automatiskt |
| Notifiering | Slack | Automatisk rapport efter varje modellkörning |

---

## 2. Dataset – NSL-KDD

### Bakgrund

NSL-KDD är en förbättrad version av det klassiska KDD Cup 1999-datasetet, framtaget av University of New Brunswick. Det är ett av de mest använda dataseten inom forskning om intrångsdetekteringssystem (IDS).

Förbättringarna jämfört med KDD'99:
- Inga dubbletter i träningssetet — modellen blir inte skev mot vanliga poster
- Svårighetspoäng per rad gör det möjligt att analysera modellens prestanda på lätt vs svår data
- Testdatan kommer från en annan fördelning än träningsdatan, vilket gör uppgiften mer realistisk

### Filer

| Fil | Rader | Storlek | Används |
|---|---|---|---|
| `KDDTrain+.txt` | 125 973 | ~18 MB | ✅ Hela pipelinen |
| `KDDTest+.txt` | 22 544 | ~3.8 MB | Ej använd — se not nedan |
| `KDDTrain+_20Percent.txt` | 25 192 | ~3.6 MB | Valfritt |
| `KDDTest-21.txt` | 11 850 | ~2 MB | Valfritt |

> **Om valet av utvärderingsmetod**
>
> Endast `KDDTrain+.txt` laddas upp till Snowflake. Modellerna utvärderas på en
> 80/20-split av den filen (100 778 träning / 25 195 test), inte mot `KDDTest+.txt`.
>
> Det är värt att vara tydlig med, eftersom det förklarar de höga F1-värdena.
> `KDDTest+` är medvetet dragen från en annan fördelning än träningsdatan och innehåller
> attacktyper som inte förekommer i träningssetet. Utvärdering mot den mäter alltså
> generalisering till *okända* attacker, vilket är en svårare uppgift — publicerade
> Random Forest-resultat på den splitten ligger typiskt kring 77–82 % accuracy, mot
> 99,5 % här.
>
> En in-distribution-split mäter hur väl modellen lärt sig mönstren i den data den
> tränats på. Det är en giltig utvärdering av modellen som sådan, men den säger mindre
> om hur den skulle klara helt nya attacktyper i produktion. En vidareutveckling av
> projektet vore att utvärdera mot `KDDTest+` och jämföra de två måtten.

### Kolumnstruktur (43 kolumner)

Datasetet har inga kolumnrubriker i filen — dessa läggs till manuellt vid uppladdning.

**Grundläggande features (col 1–9) — TCP/IP-attribut:**

| # | Kolumn | Typ | Beskrivning |
|---|---|---|---|
| 1 | `duration` | Numerisk | Anslutningens längd i sekunder |
| 2 | `protocol_type` | Kategorisk | tcp / udp / icmp |
| 3 | `service` | Kategorisk | http, ftp, smtp, telnet... |
| 4 | `flag` | Kategorisk | Anslutningsstatus: SF, S0, REJ, RSTO... |
| 5 | `src_bytes` | Numerisk | Bytes skickade från källa |
| 6 | `dst_bytes` | Numerisk | Bytes skickade till destination |
| 7 | `land` | Binär | 1 om källa = destination |
| 8 | `wrong_fragment` | Numerisk | Antal felaktiga fragment |
| 9 | `urgent` | Numerisk | Antal urgenta paket |

**Innehållsfeatures (col 10–22) — misstänkt beteende:**

| # | Kolumn | Typ | Beskrivning |
|---|---|---|---|
| 10 | `hot` | Numerisk | Antal misstänkta kommandon |
| 11 | `num_failed_logins` | Numerisk | Misslyckade inloggningsförsök |
| 12 | `logged_in` | Binär | 1 om användaren är inloggad |
| 13 | `num_compromised` | Numerisk | Antal komprometterade villkor |
| 14 | `root_shell` | Binär | 1 om root shell erhölls |
| 15 | `su_attempted` | Binär | 1 om su-kommando försöktes |
| 16 | `num_root` | Numerisk | Antal root-åtkomster |
| 17 | `num_file_creations` | Numerisk | Antal skapade filer |
| 18 | `num_shells` | Numerisk | Antal shell-kommandon |
| 19 | `num_access_files` | Numerisk | Åtkomster till känsliga filer |
| 20 | `num_outbound_cmds` | Numerisk | Utgående kommandon i ftp-session |
| 21 | `is_host_login` | Binär | 1 om host-inloggning |
| 22 | `is_guest_login` | Binär | 1 om gästinloggning |

**Trafikfeatures (col 23–41) — baserade på fönster av 100 anslutningar:**

Dessa features beskriver mönster i nätverkstrafiken snarare än enskilda anslutningar — exempelvis hur stor andel av de senaste 100 anslutningarna som gick till samma tjänst, eller hur hög felrate som observerades.

Nyckelvaribler: `count`, `srv_count`, `serror_rate`, `rerror_rate`, `same_srv_rate`, `diff_srv_rate`, `dst_host_count`, `dst_host_srv_count` m.fl.

**Målvariabel och metadata:**

| # | Kolumn | Beskrivning |
|---|---|---|
| 42 | `label` | Attacktyp eller "normal" |
| 43 | `difficulty_level` | Svårighetspoäng 1–21 (används ej i ML) |

### Attackkategorier

| Kategori | Specifika attacker | Antal i träningsdata |
|---|---|---|
| **Normal** | — | 67 343 |
| **DoS** | neptune, smurf, back, land, pod, teardrop | ~45 900 |
| **Probe** | satan, ipsweep, portsweep, nmap | ~11 656 |
| **R2L** | warezclient, guess_passwd, warezmaster, imap... | ~1 000 |
| **U2R** | buffer_overflow, rootkit, perl, loadmodule | ~52 |

> U2R är kraftigt underrepresenterad (52 rader av 125 973) — detta påverkar modellens förmåga att detektera denna attacktyp och är värt att notera i resultatavsnittet.

---

## 3. Snowflake – Data Warehouse

### Syfte

Snowflake fungerar som projektets centrala datalager. Rådata laddas upp en gång och är sedan tillgänglig för dbt och Databricks utan att behöva hanteras lokalt.

### Konfiguration

| Parameter | Värde |
|---|---|
| Edition | Enterprise (30-dagars trial) |
| Cloud | Microsoft Azure |
| Region | Sweden Central (Gävle) |
| Account identifier | `MOMHVMC-KB20003` |
| Användare | `TT8010` |
| Warehouse | COMPUTE_WH (X-Small) |
| Databas | `NSL_KDD_DB` |
| Schema | `PUBLIC` |
| Tabell | `RAW_NETWORK_LOGS` |

### Tabellskapande

Tabellen skapades manuellt med explicita kolumntyper för alla 43 kolumner innan uppladdning, för att säkerställa korrekt datatypsmappning:

```sql
CREATE DATABASE IF NOT EXISTS NSL_KDD_DB;
USE DATABASE NSL_KDD_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

CREATE OR REPLACE TABLE NSL_KDD_DB.PUBLIC.RAW_NETWORK_LOGS (
    duration            INTEGER,
    protocol_type       VARCHAR(10),
    service             VARCHAR(20),
    flag                VARCHAR(10),
    src_bytes           INTEGER,
    dst_bytes           INTEGER,
    land                INTEGER,
    wrong_fragment      INTEGER,
    urgent              INTEGER,
    hot                 INTEGER,
    num_failed_logins   INTEGER,
    logged_in           INTEGER,
    num_compromised     INTEGER,
    root_shell          INTEGER,
    su_attempted        INTEGER,
    num_root            INTEGER,
    num_file_creations  INTEGER,
    num_shells          INTEGER,
    num_access_files    INTEGER,
    num_outbound_cmds   INTEGER,
    is_host_login       INTEGER,
    is_guest_login      INTEGER,
    count               INTEGER,
    srv_count           INTEGER,
    serror_rate         FLOAT,
    srv_serror_rate     FLOAT,
    rerror_rate         FLOAT,
    srv_rerror_rate     FLOAT,
    same_srv_rate       FLOAT,
    diff_srv_rate       FLOAT,
    srv_diff_host_rate  FLOAT,
    dst_host_count      INTEGER,
    dst_host_srv_count  INTEGER,
    dst_host_same_srv_rate      FLOAT,
    dst_host_diff_srv_rate      FLOAT,
    dst_host_same_src_port_rate FLOAT,
    dst_host_srv_diff_host_rate FLOAT,
    dst_host_serror_rate        FLOAT,
    dst_host_srv_serror_rate    FLOAT,
    dst_host_rerror_rate        FLOAT,
    dst_host_srv_rerror_rate    FLOAT,
    label               VARCHAR(30),
    difficulty_level    INTEGER
);
```

### Datavalidering

Efter uppladdning verifierades att alla rader laddades korrekt:

```sql
-- Radantal
SELECT COUNT(*) FROM NSL_KDD_DB.PUBLIC.RAW_NETWORK_LOGS;
-- Resultat: 125 973 ✅

-- Attackfördelning
SELECT label, COUNT(*) as antal
FROM NSL_KDD_DB.PUBLIC.RAW_NETWORK_LOGS
GROUP BY label
ORDER BY antal DESC
LIMIT 15;
```

**Resultat av attackfördelning:**

| Label | Antal |
|---|---|
| normal | 67 343 |
| neptune | 41 214 |
| satan | 3 633 |
| ipsweep | 3 599 |
| portsweep | 2 931 |
| smurf | 2 646 |
| nmap | 1 493 |
| back | 956 |
| teardrop | 892 |
| warezclient | 890 |
| pod | 201 |
| guess_passwd | 53 |
| buffer_overflow | 30 |
| warezmaster | 20 |
| land | 18 |

### Driftsättningsnoteringar

- Compute-motorn (COMPUTE_WH) pausas efter varje session för att spara trial-krediter
- Filen `KDDTrain+.txt` saknar header-rad — kolumnnamnen definierades i CREATE TABLE-steget
- `difficulty_level` (col 43) laddades upp men används inte i ML-modellen

---

---

## 4. dbt – Transformationer

### Syfte

dbt (data build tool) är transformationslagret mellan rådata i Snowflake och ML-modellen i Databricks. Istället för att rensa data direkt i Databricks-notebooken hålls all transformationslogik i separata SQL-filer som är versionshanterade och återanvändbara.

### Projektstruktur

```
nsl_kdd_project/
└── models/
    ├── sources.yml          # Pekar på rådata i Snowflake
    └── staging/
        ├── stgrawlogs.sql   # Rensar och standardiserar rådata
        └── fct_features.sql # Skapar ML-redo features
```

### Konfiguration

| Parameter | Värde |
|---|---|
| Projekt | `nsl_kdd_project` |
| Anslutning | Snowflake (`MOMHVMC-KB20003`) |
| Databas | `NSL_KDD_DB` |
| Warehouse | `COMPUTE_WH` |
| Output-schema | `DBT_JDOE` (dbt skapar automatiskt) |

### Fil 1: sources.yml

Definierar var rådata finns i Snowflake så att dbt-modellerna kan referera till den med `{{ source() }}`:

```yaml
version: 2

sources:
  - name: nsl_kdd
    description: "NSL-KDD nätverksintrångsdata i Snowflake"
    database: NSL_KDD_DB
    schema: PUBLIC
    tables:
      - name: RAW_NETWORK_LOGS
        description: "125 973 nätverksanslutningar med attacklabels"
        columns:
          - name: label
            description: "Attacktyp eller normal"
          - name: protocol_type
            description: "tcp, udp eller icmp"
          - name: flag
            description: "Anslutningsstatus SF, S0, REJ etc"
```

### Fil 2: stgrawlogs.sql

Stagingmodellen rensar rådata och väljer ut relevanta kolumner. `difficulty_level` exkluderas medvetet eftersom det är ett internt rankingsvärde som inte ska användas som feature:

```sql
SELECT
    CONCAT(protocol_type, '_', service, '_', flag) AS session_id,
    label,
    duration,
    protocol_type,
    service,
    flag,
    src_bytes,
    dst_bytes,
    land,
    wrong_fragment,
    urgent,
    hot,
    num_failed_logins,
    logged_in,
    num_compromised,
    root_shell,
    su_attempted,
    num_root,
    num_file_creations,
    num_shells,
    num_access_files,
    num_outbound_cmds,
    is_host_login,
    is_guest_login,
    count,
    srv_count,
    serror_rate,
    srv_serror_rate,
    rerror_rate,
    srv_rerror_rate,
    same_srv_rate,
    diff_srv_rate,
    srv_diff_host_rate,
    dst_host_count,
    dst_host_srv_count,
    dst_host_same_srv_rate,
    dst_host_diff_srv_rate,
    dst_host_same_src_port_rate,
    dst_host_srv_diff_host_rate,
    dst_host_serror_rate,
    dst_host_srv_serror_rate,
    dst_host_rerror_rate,
    dst_host_srv_rerror_rate
    -- difficulty_level exkluderas medvetet
FROM {{ source('nsl_kdd', 'RAW_NETWORK_LOGS') }}
```

### Fil 3: fct_features.sql

Huvudmodellen lägger till två nya kolumner som ML-modellen använder direkt — `attack_category` (mappning från specifik attacknamn till en av fem kategorier) och `is_attack` (binär 0/1):

```sql
SELECT
    session_id,
    label,
    CASE
        WHEN label = 'normal' THEN 'normal'
        WHEN label IN ('neptune','back','land','pod','smurf','teardrop',
                       'apache2','mailbomb','processtable','udpstorm') THEN 'dos'
        WHEN label IN ('ipsweep','nmap','portsweep','satan',
                       'mscan','saint') THEN 'probe'
        WHEN label IN ('ftp_write','guess_passwd','imap','multihop',
                       'phf','spy','warezclient','warezmaster','sendmail',
                       'named','snmpgetattack','snmpguess','xlock',
                       'xsnoop','worm') THEN 'r2l'
        WHEN label IN ('buffer_overflow','loadmodule','perl','rootkit',
                       'httptunnel','ps','sqlattack','xterm') THEN 'u2r'
        ELSE 'unknown'
    END AS attack_category,
    CASE WHEN label = 'normal' THEN 0 ELSE 1 END AS is_attack,
    -- ... alla features från stgrawlogs
FROM {{ ref('stgrawlogs') }}
```

### Körning och resultat

```bash
dbt run
# Pass: 2  Warn: 0  Error: 0
```

Verifiering i Snowflake (`NSL_KDD_DB.DBT_JDOE.FCT_FEATURES`):

```sql
SELECT COUNT(*) FROM NSL_KDD_DB.DBT_JDOE.FCT_FEATURES;
-- 125 973 ✅

SELECT attack_category, COUNT(*) as antal
FROM NSL_KDD_DB.DBT_JDOE.FCT_FEATURES
GROUP BY attack_category ORDER BY antal DESC;
```

| attack_category | antal |
|---|---|
| normal | 67 343 |
| dos | 45 927 |
| probe | 11 656 |
| r2l | 995 |
| u2r | 52 |

| is_attack | antal |
|---|---|
| 0 (normal) | 67 343 |
| 1 (attack) | 58 630 |

### Noteringar

- dbt skapar views (inte tabeller) som standard — detta är effektivt under utveckling
- Output-schemat `DBT_JDOE` skapas automatiskt av dbt baserat på användarnamnet
- U2R-kategorin är kraftigt underrepresenterad (52 rader) vilket kommer påverka modellens recall för denna attacktyp

---

---

## 5. Databricks – ML-modell

### Syfte

Databricks är ML-plattformen där data hämtas från Snowflake, förbereds för träning och där ML-modellerna byggs och utvärderas. Community Edition används vilket är permanent gratis och kräver inget betalkort.

### Konfiguration

| Parameter | Värde |
|---|---|
| Miljö | Databricks Community Edition |
| Compute | Serverless |
| Notebook | `NSL_KDD_Pipeline` |
| Språk | Python |

### Cell 1 — installera bibliotek

```python
%pip install snowflake-connector-python pandas scikit-learn -q
%restart_python
```

### Cell 2 — anslut till Snowflake

```python
import snowflake.connector
import pandas as pd
from getpass import getpass

conn = snowflake.connector.connect(
    account='MOMHVMC-KB20003',
    user='TT8010',
    password=getpass('Snowflake-lösenord: '),
    database='NSL_KDD_DB',
    schema='DBT_JDOE',
    warehouse='COMPUTE_WH'
)
```

Anslutningen använder `getpass` för att lösenordet aldrig sparas i koden — viktigt för säkerhet och GitHub-uppladdning.

### Cell 3 — hämta data

```python
query = "SELECT * FROM NSL_KDD_DB.DBT_JDOE.FCT_FEATURES"
df = pd.read_sql(query, conn)
conn.close()
```

**Resultat:** 125 973 rader, 45 kolumner inladdade som pandas DataFrame.

### Cell 4 — förbered features

38 numeriska kolumner valdes ut som features. Kategoriska kolumner (`protocol_type`, `service`, `flag`) exkluderades.

```python
X = df[numeriska_kolumner]   # 38 numeriska features
y = df['IS_ATTACK']          # 0 = normal, 1 = attack

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled  = scaler.transform(X_test)
```

**Fördelning i träningsdata:**
- Normal (0): 53 921 rader
- Attack (1): 46 857 rader

**Varför StandardScaler?** Utan normalisering dominerar kolumner med stora tal (t.ex. `src_bytes`) över kolumner med små tal (t.ex. `root_shell` som bara är 0 eller 1). StandardScaler ger alla features samma utgångsskala genom formeln `(värde - medelvärde) / standardavvikelse`.

### Cell 5 — träna och jämföra modeller

Två modeller tränades för att jämföra övervakad vs oövervakad ML:

**Isolation Forest (oövervakad):** Tränas utan labels och hittar datapunkter som avviker från mängden. `contamination=0.46` anger förväntad andel attacker.

**Random Forest (övervakad):** Bygger 100 beslutsträd som röstar om varje förutsägelse. Tränas med labels och lär sig direkt vad som är attack vs normal.

**Resultat:**

| Modell | F1-score | Typ |
|---|---|---|
| Isolation Forest | 0.642 | Oövervakad |
| Random Forest v1 | 0.995 | Övervakad |

Random Forest vinner tydligt eftersom den lär sig från labels under träningen.

---

## 6. MLflow – Experiment tracking

### Syfte

MLflow loggar automatiskt alla modellkörningar — parametrar, metrics och själva modellfilerna. Detta gör det möjligt att jämföra modellversioner och återskapa exakta resultat.

### Experiment

Experiment: `/Users/tomthorsen1@gmail.com/nsl_kdd_intrusion_detection`

| Run Name | F1-score | Status |
|---|---|---|
| Random_Forest_v1 | 0.995 | ✅ |
| Random_Forest_v2 | 0.997 | ✅ |
| Isolation_Forest | 0.642 | ✅ |

**Random Forest v1 Run ID:** `bec5283bb0b8484ab18b7ac175c119ec`

### Loggning

```python
with mlflow.start_run(run_name="Random_Forest_v1"):
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_metric("f1_score", rf_f1)
    mlflow.log_metric("precision", precision_score(y_test, rf_pred))
    mlflow.log_metric("recall", recall_score(y_test, rf_pred))
    mlflow.sklearn.log_model(rf_model, "random_forest_model")
    rf_run_id = mlflow.active_run().info.run_id
```

---

## 8. A/B-testning (VG)

### Syfte

A/B-testning jämför två modellversioner på riktig trafik innan full utrullning. Används i produktion av Spotify, Netflix och Uber för att minimera risken med nya modeller.

### Upplägg

| Version | n_estimators | max_depth | Trafikandel |
|---|---|---|---|
| v1 (A) | 100 | 10 | 70% |
| v2 (B) | 200 | 15 | 30% |

Trafiken routades deterministiskt med hash-baserad fördelning — samma anslutning går alltid till samma modellversion. **Varför inte 50/50?** I produktion minimerar man exponeringen mot en potentiellt sämre modell och ökar gradvis om resultaten är bra.

### Resultat

| Version | F1-score | Korrekthet | Antal rader |
|---|---|---|---|
| v1 (100 träd, djup 10) | 0.995 | 0.995 | 17 638 |
| v2 (200 träd, djup 15) | 0.997 | 0.998 | 7 557 |

### Analys

V2 presterar marginellt bättre (0.002 skillnad i F1-score) men kräver dubbelt så många träd och längre träningstid. Skillnaden är för liten för att motivera bytet — **v1 behålls i produktion**.

Detta illustrerar en viktig princip i MLOps: en mer komplex modell är inte alltid bättre. Enkelhet och lägre resurskostnad väger tungt i produktionsbeslut.

### Beslutsregeln i kod

Avvägningen är inte en ren metrikjämförelse — den väger förbättring mot driftkostnad. Därför är den implementerad som en explicit tröskel istället för en rak `>`-jämförelse:

```python
# En mer komplex modell måste betala för sig. v2 har dubbelt så många
# träd (200 vs 100) och 50% djupare träd — det kostar både tränings-
# och inferenstid i produktion. Vi kräver en minsta förbättring.
MIN_FORBATTRING = 0.01

forbattring = rf_f1_v2 - rf_f1

if forbattring >= MIN_FORBATTRING:
    rekommendation = "Uppgradera till Version B (v2)"
else:
    rekommendation = "Behåll Version A (v1) i produktion"
```

Utfall:

```
Skillnad i F1:  +0.0018
Tröskel:        +0.0100
Rekommendation: Behåll Version A (v1) i produktion
```

Tröskelvärdet 0.01 är ett omdöme, inte en universell konstant — rätt nivå beror på hur dyr modellen är att köra och vad varje procentenhet är värd i sammanhanget. Poängen är att kriteriet är uttalat och synligt i koden, istället för att beslutet fattas utanför den. Samma variabel `rekommendation` återanvänds i Slack-rapporten, så utskrift, rapport och slutsats aldrig kan glida isär.

---

---

## 9. Automatisering – Slack-rapport

### Syfte

Efter varje modellkörning skickas en automatisk rapport till Slack-kanalen `#ml-alerts`. Detta är hur moderna ML-team i produktion håller koll på modellprestanda utan att behöva logga in i Databricks manuellt.

### Slack-konfiguration

| Parameter | Värde |
|---|---|
| Workspace | `nsl-kdd-project` |
| Kanal | `#ml-alerts` |
| App | `ML Alerts` |
| Metod | Incoming Webhook |

Webhook skapades via api.slack.com/apps → Incoming Webhooks → Add New Webhook to Workspace → `#ml-alerts`.

### Kod

```python
import requests
import json

# OBS: Webhook-URL lagras aldrig i GitHub — ersätts med platshållare
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/XXX/YYY/ZZZ"

rapport = f"""
*NSL-KDD Intrusion Detection — Modellrapport*
================================================
*Dataset:* 125 973 nätverksanslutningar
*Träningsdata:* 100 778 rader (80%)
*Testdata:* 25 195 rader (20%)

*Modellresultat:*
• Isolation Forest F1-score: {iso_f1:.3f}
• Random Forest v1 F1-score: {rf_f1:.3f}
• Random Forest v2 F1-score: {rf_f1_v2:.3f}

*A/B-test (70/30 trafikdelning):*
• Version A (v1) korrekthet: {v1_correct:.3f}
• Version B (v2) korrekthet: {v2_correct:.3f}
• Rekommendation: {rekommendation}

*MLflow Run ID (RF v1):* {rf_run_id}
*Status:* Klar
"""

response = requests.post(
    SLACK_WEBHOOK_URL,
    data=json.dumps({"text": rapport}),
    headers={"Content-Type": "application/json"}
)
```

### Resultat

Rapporten skickades korrekt till `#ml-alerts` med alla modellresultat, A/B-test och MLflow Run ID.

### Säkerhetsnotering

Webhook-URL:en är en hemlighet och får aldrig committas till GitHub. I koden ersätts den med `XXX/YYY/ZZZ` innan uppladdning. I ett riktigt produktionssystem lagras den som en miljövariabel eller i ett secrets management-system som AWS Secrets Manager eller Databricks Secrets.

---

---

## 10. Dashboard – Databricks SQL

### Syfte

Dashboarden visualiserar modellprestanda över tid och gör det enkelt att snabbt se om modellen förbättras eller försämras efter varje körning. Skapad med Databricks inbyggda dashboard-verktyg.

### Datakälla

En permanent Delta-tabell `model_performance_history` skapades i notebooken med alla modellkörningar:

```python
spark_df = spark.createDataFrame(historik)
spark_df.write.mode("overwrite").saveAsTable("model_performance_history")
```

### Visualiseringar

Dashboarden innehåller två widgets skapade med Genie Code-assistenten:

**1. Model Performance Metrics — stapeldiagram**
Visar F1-score, Precision och Recall för varje modellkörning sida vid sida. Isolation Forest syns tydligt sämre (~0.64) jämfört med Random Forest-versionerna (~1.0).

**2. Model Performance Data — tabell**
Visar alla kolumner: Run Name, Model Type, N Estimators, Max Depth, F1 Score, Precision, Recall, Run Date.

### Resultat i dashboarden

| Run Name | Model Type | N Estimators | Max Depth | F1 Score | Precision | Recall |
|---|---|---|---|---|---|---|
| Isolation_Forest | IsolationForest | null | null | 0.64 | 0.65 | 0.64 |
| Random_Forest_v1 | RandomForest | 100 | 10 | 1.00 | 1.00 | 0.99 |
| Random_Forest_v2 | RandomForest | 200 | 15 | 1.00 | 1.00 | 1.00 |

Dashboarden publicerades med **Share data permission** så att alla med länken kan se den utan att behöva ett eget Databricks-konto.

---

## 11. Resultat och analys

### Modellprestanda

| Modell | F1-score | Precision | Recall | Typ |
|---|---|---|---|---|
| Isolation Forest | 0.642 | 0.646 | 0.639 | Oövervakad |
| Random Forest v1 | 0.995 | 0.999 | 0.992 | Övervakad |
| Random Forest v2 | 0.997 | 0.999 | 0.995 | Övervakad |

### Viktigaste lärdomar

**Random Forest dominerar tydligt** — F1-score på 0.995 mot Isolation Forests 0.642. Den övervakade modellen som lär sig från labels är överlägsen för detta dataset.

**A/B-testning visade att komplexitet inte alltid lönar sig** — V2 med dubbelt så många träd presterade bara 0.002 bättre i F1-score. V1 behålls i produktion av kostnadsskäl.

**U2R är den svåraste kategorin** — med bara 52 träningsexempel av 125 973 är modellen sannolikt sämre på att detektera User-to-Root-attacker. I produktion skulle man behöva fler träningsexempel för denna kategori.

**dbt som transformationslager fungerade väl** — separationen mellan rådata, staging och feature-tabell gjorde koden modulär och lätt att underhålla.

### Arkitekturens styrkor

Denna pipeline följer industristandard för ML i produktion:
- Snowflake hanterar rådata skalbart och tillgängligt
- dbt håller transformationslogiken versionshanterad och återanvändbar
- MLflow ger full spårbarhet av alla experiment
- Slack-rapporten automatiserar övervakning utan manuellt arbete
- A/B-testning minimerar risken vid modellupdateringar

---

## 12. GitHub

### Repository

**URL:** `https://github.com/TWTCT80/nsl-kdd-intrusion-detection`

### Struktur

```
nsl-kdd-intrusion-detection/
├── .gitignore                          # Exkluderar secrets, CSV-filer och .pkl
├── README.md                           # Projektöversikt och resultatsammanfattning
├── dbt_models/
│   └── models/
│       ├── sources.yml                 # Snowflake-källkonfiguration
│       ├── fct_features.sql            # ML-features med attackkategorier
│       └── staging/
│           └── stgrawlogs.sql          # Staging-modell
├── notebooks/
│   └── NSL_KDD_Pipeline.ipynb          # Databricks-notebook med all ML-kod
└── docs/
    └── MLPipeline_NSL_KDD_Writeup.md   # Denna writeup
```

### Säkerhet

Följande finns **inte** i repot:
- Snowflake-lösenord — anges via `getpass()` vid körning
- Slack webhook-URL — ersatt med `XXX/YYY/ZZZ` i notebook
- NSL-KDD CSV-filer — exkluderade via `.gitignore` (finns i Snowflake)
- Modell-pickle-filer — sparade i MLflow, inte i Git

### Commits

| Commit | Beskrivning |
|---|---|
| `8a5a317` | Initial commit: NSL-KDD intrusion detection pipeline |
| `609e7b9` | Add dbt models: stgrawlogs and fct_features |
| *(sista)* | Add final writeup |

---

*Projekt avslutat: 2026-06-11*  
*GitHub: https://github.com/TWTCT80/nsl-kdd-intrusion-detection*
