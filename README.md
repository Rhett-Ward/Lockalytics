# Lockalytics
Big data analytics pipeline and live dashboard for the game Deadlock (Valve), built for CS 4265: Big Data Analytics at KSU.

Pulls from the community Deadlock API, stores raw JSON in Azure Blob Storage, processes it with PySpark, loads results into a self-hosted MongoDB instance, and serves it through a web dashboard that auto-refreshes every 30 seconds.

**Live dashboard:** `lockalytics.tech`

DISCLAIMER: Readability of data is still very actively being worked on, it is being processed and moved and sorted and transfered but somethings like hero and ability ID's are still in the process of being written over the actual number.

---

## Table of Contents
- [How It Works](#how-it-works)
- [Requirements](#requirements)
- [Setup](#setup)
- [Running It](#running-it)
- [Data Dictionary](#data-dictionary)
- [Project Structure](#project-structure)
- [Progress Reports](#progress-reports)

---

## How It Works

```
[Deadlock Community API]
         │
         │  fetch_and_store_4.py  (cron: every 15 min)
         ▼
[Azure Blob Storage — ADLS Gen2]
  raw/{endpoint_name}/YYYY/MM/DD/HHMMSS.json
         │
         │  spark_process1.py  (cron: every 20 min)
         ▼
[MongoDB 7.0 — self-hosted on VPS]
  db: lockalytics  |  14+ collections
         │
         ▼
[Express — server1.js, port 3000]
         │
         ▼
[heroes1.html — live dashboard]
```

`fetch_and_store_4.py` runs in 6 beats covering the Deadlock API: global analytics, per-hero analytics, builds, match metadata, scoreboards, and player data. Each response gets wrapped in a JSON envelope with a timestamp and record count and uploaded to Azure. `spark_process1.py` then reads those blobs, aggregates or flattens the data depending on the endpoint, and overwrites the corresponding MongoDB collection. The Express server reads from MongoDB and the dashboard polls it on a 30-second interval.

The whole thing runs on a Hostinger VPS (Ubuntu 22.04) managed with PM2 for the web server and cron for the two Python scripts. (also using cron to restart the pm2 server at a 30 minute interval for hosting health)

---

## Requirements

The deployed version is already running continuously, so you only need this if you want to run your own instance.

- **Python 3.10+**
- **Java 11** -- required by Spark (`sudo apt install openjdk-11-jdk`)
- **Apache Spark 3.5.x** with bundled Hadoop 3.3 -- [download here](https://spark.apache.org/downloads.html), extract to `/opt/spark`
- **Azure Storage Account** (ADLS Gen2) -- you'll need your own account, key, and connection string
- **MongoDB 7.0** -- self-hosted or Atlas both work, just update the URI in `.env`
- **Node.js v20+** -- recommend installing via [nvm](https://github.com/nvm-sh/nvm)

---

## Setup

### Step 1 -- Clone the repo and set up a virtual environment

```bash
git clone https://github.com/Rhett-Ward/Lockalytics.git
cd lockalytics
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 2 -- Configure your `.env`

Copy the example file and fill in your credentials:

```bash
cp .env.example .env
```

```env
AZURE_STORAGE_ACCOUNT_NAME=your_account_name
AZURE_STORAGE_ACCOUNT_KEY=your_key_here
AZURE_CONTAINER_NAME=deadlock-data
AZURE_CONNECTION_STRING=your_connection_string_here

MONGO_URI=mongodb://username:password@127.0.0.1:27017/?authSource=lockalytics
MONGO_DB_NAME=lockalytics
MONGO_COLLECTION=hero_stats

PORT=3000
```

The `?authSource=lockalytics` on the Mongo URI matters if your user was created inside the `lockalytics` database rather than `admin`. If you set it up differently, adjust accordingly.

### Step 3 -- Spark environment variables

Add these to your shell profile or just export them before running:

```bash
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=~/lockalytics/venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/lockalytics/venv/bin/python
```

The `PYSPARK_PYTHON` lines are important. Without them Spark will use the system Python and immediately fail on missing imports.

### Step 4 -- Azure connector JARs

Spark pulls the MongoDB connector automatically via `--packages` on first run. The Azure JARs need to be placed in `$SPARK_HOME/jars/` manually:

- `hadoop-azure-3.3.4.jar`
- `azure-storage-8.6.6.jar`
- `wildfly-openssl-1.0.7.Final.jar`

### Step 5 -- Node dependencies

```bash
npm install
```

---

## Running It

### Ingestion

```bash
source venv/bin/activate
python fetch_and_store_4.py
```
<img width="761" height="406" alt="image" src="https://github.com/user-attachments/assets/fd0ecdd9-66d8-4b98-90c6-6d7d40a1f9e8" />


Runs through all 6 phases and logs `OK`, `SKIP`, or `FAIL` per endpoint to stdout. A full run takes a few minutes depending on how many matches and accounts get discovered in the match metadata phase.

### Spark

```bash
spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=10 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 spark_process.py
```

<img width="618" height="308" alt="image" src="https://github.com/user-attachments/assets/3d02a40e-3e1c-4010-afdc-e3ea5ff84d04" />



Loops through all endpoint folders in blob storage, processes each one, and writes to the corresponding MongoDB collection. Endpoints with no blob data yet are skipped without crashing the run.

### Web server

```bash
pm2 start server.js --name lockalytics-web2

# or if it's already running
pm2 restart lockalytics-web2 --update-env
```
to boot up webserver if you want to host a web based endpoint

### Cron setup

```
*/15 * * * * /root/lockalytics/venv/bin/python /root/lockalytics/fetch_and_store_4.py

*/20 * * * * /root/lockalytics/spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=10 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 spark_process.py
```
For if you want to automate it yourself
---

## Data Dictionary

All collections live in the `lockalytics` MongoDB database. Every raw blob in Azure uses the same wrapper format before Spark processes it:

```json
{
  "fetched_at": "2026-04-05T10:00:00+00:00",
  "endpoint":   "hero_stats",
  "record_count": 62,
  "data": [ ... ]
}
```

### hero_stats

Aggregated across all fetch runs. Spark groups by `hero_id` and sums wins/losses/matches rather than storing a raw snapshot each time.

| Field | Type | Description |
|---|---|---|
| `hero_id` | int | Hero identifier from the Deadlock API |
| `total_wins` | long | Wins summed across all fetched snapshots |
| `total_losses` | long | Losses summed across all fetched snapshots |
| `total_matches` | long | Total matches |
| `win_rate` | double | `total_wins / total_matches`, 4 decimal places |
| `last_updated` | string | ISO 8601 timestamp of the most recent contributing fetch |

### item_stats

Same aggregation as `hero_stats`, keyed on items instead.

| Field | Type | Description |
|---|---|---|
| `item_id` | int | Item identifier |
| `total_wins` | long | Wins summed across all snapshots |
| `total_losses` | long | Losses summed |
| `total_matches` | long | Total matches |
| `win_rate` | double | `total_wins / total_matches`, 4 decimal places |
| `last_updated` | string | Timestamp of most recent contributing fetch |

### bulk_metadata / match_metadata

`bulk_metadata` is the top-level batch response from the API. `match_metadata` is the per-match drill-down fetched for each discovered `match_id`. Both go through the generic flatten path in Spark.

| Field | Type | Description |
|---|---|---|
| `match_id` | int | Unique match identifier |
| `players` | array | Player objects containing `account_id` and per-player stats |
| `fetched_at` | string | Fetch timestamp |

### match_history

Per-player match history. Fetched with `only_stored_history=True` to stay within the free API rate limits. `account_id` is injected by the ingestion script since the discovery happens in the match metadata phase.

| Field | Type | Description |
|---|---|---|
| `account_id` | int | Player account ID |
| `match_id` | int | Match this record belongs to |
| `hero_id` | int | Hero the player used |
| `fetched_at` | string | Fetch timestamp |

### Everything else

`hero_counter_stats`, `hero_synergy_stats`, `hero_comb_stats`, `badge_distribution`, `kill_death_stats`, `player_performance_curve`, `player_stat_metrics`, `ability_order_stats`, `build_item_stats`, `search_builds`, `hero_scoreboard`, and `player_scoreboard` all go through the generic Spark path: the `data` array gets exploded into one row per record and struct fields get promoted to top-level columns. Field names come through from the API response as-is.

---

## Project Structure

```
lockalytics/
├── fetch_and_store_4.py     # Ingestion script
├── spark_process1.py       # Spark processing script
├── server1.js              # Express web server
├── heroes1.html            # Dashboard
├── requirements.txt        # Python deps
├── package.json            # Node deps
```

---

# Progress Reports

2/16/2026-

First successful api call from Deadlock community API

Exported data call to a csv file via python and created a google sheets script to begin converting some data to be readable and consumable.

worked with api dev to spot some bugs.

emailed prof for rescoping advice.

2/24/2026-

got a test db running using atlas

modified my existing python api test script to access said db cluster and add data as a collection

successfully imported csv of hero stats into cluster from python script

3/3/2026

deployed a hadoop hdfs docker container and began learning how to interact with it.

next steps:

setup apache spark to stream from api (first iteration might just be making calls on a timer or making a call with pyspark at all)

Attached HDFS container to AZUR blob storage

create mongodb atlas instance through azure that is linked to that blob storage

rewrite project statement

3/6/2026

made a batch script that connects everything together, has requirements in it, can auto install just about everything (just need docker, npm, and python before hand) and does all the work

the end result is a hdfs and mongo db cluster that host a csv and a website that pulls from it and is created inside of a node js script. have to figure out how to upload it all to github due to data limits. 

have to write report still

have to figure out what can and cant be uploaded (like atlas db key)

3/8/2026

Finished Initial implementation report. Did read me write up. Refactored batch file to run for anyone and not use specifics for my pc.

3/19/2026

Beginning full transitional process to automation and fully online hosting.

Purchased server to host the python script, the web app, the mongodb

Setup Azure blob storage to be the HDFS moving away from docker

Setup self hosted mongodb instance as opposed to using Atlas for size and cost reasons.

worked through lots of debugging. Got first set of raw data into Azure container.

got a sample express server up and running as a tester. 

started working on spark implementation in order to process data out of azure and into the self hosted mongodb instance.

started reworking python fetch script. to do: change dump location to azure blob rather then to mongo DB

4/4/2026

Completely debugged spark test pipeline thing. Was able to properly prove transfer into mongodb and from there into azure.

These issues included but not limited to: Running system python instead of venv python instance and therefore missing dependencies, Misspelled credentials, poor file search.

04/05/2026

Refactoring github to prepare for full upload of project and submission.

Changing python fetch and store script to fetch and store many api calls at once rather then just one (functionality tested across the board just needs to be implemented together)

setting up cron

finished spark_process1.py to match with the new collections

got to test and debug new server.js implementation (meant to put this in on 03/27/26 but I went back and redid the server.js and heroes.html to match the fields I planned on implementing.)

wrote report.




