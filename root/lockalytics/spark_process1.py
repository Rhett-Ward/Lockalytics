# spark_process.py
# Reads all endpoint folders from Azure Blob Storage, processes each one,
# and writes results to MongoDB (one collection per endpoint).
#
# Run manually:
#   spark-submit \
#     --driver-memory 4g \
#     --conf spark.sql.shuffle.partitions=10 \
#     --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
#     spark_process.py
#
# Cron (every 10 min, offset 3 min after ingestion):
#   3-59/10 * * * * /opt/spark/bin/spark-submit \
#     --driver-memory 4g \
#     --conf spark.sql.shuffle.partitions=10 \
#     --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
#     /root/lockalytics/spark_process.py >> /root/lockalytics/logs/spark.log 2>&1


import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

load_dotenv()

# --- Credentials ---
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "deadlock-data")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB_NAME", "lockalytics")

for name, val in [
    ("AZURE_STORAGE_ACCOUNT_NAME", ACCOUNT_NAME),
    ("AZURE_STORAGE_ACCOUNT_KEY", ACCOUNT_KEY),
    ("MONGO_URI", MONGO_URI),
]:
    if not val:
        print(f"ERROR: {name} not set in .env", file=sys.stderr)
        sys.exit(1)

ABFS_ROOT = f"abfss://{CONTAINER}@{ACCOUNT_NAME}.dfs.core.windows.net"


# --- Endpoint registry ---
# folder name -> processing mode
#   "hero"    = groupBy hero_id, aggregate wins/losses/matches, compute win_rate
#   "item"    = same thing but keyed on item_id
#   "generic" = explode data array, flatten struct fields, write as-is
#
# lines up with what fetch_and_store.py writes.
# match_history can arrive as multiple _batchN blobs per run,
# recursiveFileLookup picks them all up so we don't need to do anything special.

ENDPOINTS = {
    # Analytics (global)
    "hero_stats": "hero",
    "item_stats": "item",
    "hero_counter_stats": "generic",
    "hero_synergy_stats": "generic",
    "hero_comb_stats": "generic",
    "badge_distribution": "generic",
    "kill_death_stats": "generic",
    "player_performance_curve": "generic",
    "player_stat_metrics": "generic",

    # Analytics (per-hero)
    "ability_order_stats": "generic",
    "build_item_stats": "generic",

    # Builds
    "search_builds": "generic",

    # Matches
    # "shallow" mode — explode data array but skip struct flattening.
    # match_metadata has 30k records with deeply nested player arrays inside
    # match_info. Flattening that caused OOM on the VPS. MongoDB handles
    # nested documents natively so there's no reason to flatten it anyway.
    "bulk_metadata": "shallow",
    "match_metadata": "shallow",

    # Scoreboards
    "hero_scoreboard": "generic",
    "player_scoreboard": "generic",

    # Players
    # match_history also arrives in _batchN blobs but recursiveFileLookup
    # picks them all up. Shallow mode here too since records can be large.
    "match_history": "shallow",
}


# --- Spark session ---
spark = (
    SparkSession.builder
    .appName("Lockalytics-Pipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# setting azure creds on hadoop config directly since builder.config() was flaky
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set(
    f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net",
    ACCOUNT_KEY,
)


# --- Helpers ---

def read_endpoint(name):
    """Read all JSON blobs under an endpoint folder. Returns df or None."""
    path = f"{ABFS_ROOT}/raw/{name}/"
    try:
        df = (
            spark.read
            .option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .json(path)
        )
        if df.rdd.isEmpty():
            print(f"  SKIP\t{name} — folder exists but no data")
            return None
        return df
    except AnalysisException:
        print(f"  SKIP\t{name} — folder not found in blob")
        return None
    except Exception as e:
        print(f"  ERROR\t{name} — {e}")
        return None


def write_collection(df, collection):
    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .option("spark.mongodb.write.database", MONGO_DB)
        .option("spark.mongodb.write.collection", collection)
        .save()
    )


def flatten(df):
    cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for sub in field.dataType.fields:
                cols.append(
                    F.col(f"`{field.name}`.`{sub.name}`")
                     .alias(f"{field.name}_{sub.name}")
                )
        else:
            cols.append(F.col(f"`{field.name}`"))
    return df.select(cols)


# --- Processing modes ---

def process_hero(df):
    """Sum up wins/losses/matches per hero_id and calc win_rate.
    Supports both 'heroes' (old key) and 'data' (current)."""
    if "data" in df.columns:
        col = "data"
    elif "heroes" in df.columns:
        col = "heroes"
    else:
        print("    WARN: hero_stats has neither 'data' nor 'heroes' column")
        return None

    heroes = (
        df
        .select(F.col("fetched_at"), F.explode(F.col(col)).alias("hero"))
        .select(F.col("fetched_at"), F.col("hero.*"))
    )

    return (
        heroes
        .groupBy("hero_id")
        .agg(
            F.sum("wins").alias("total_wins"),
            F.sum("losses").alias("total_losses"),
            F.sum("matches").alias("total_matches"),
            F.max("fetched_at").alias("last_updated"),
        )
        .withColumn(
            "win_rate",
            F.round(F.col("total_wins") / F.col("total_matches"), 4),
        )
        .filter(F.col("total_matches") > 0)
    )


def process_item(df):
    """Same as process_hero but keyed on item_id."""
    if "data" not in df.columns:
        print("    WARN: item_stats has no 'data' column")
        return None

    items = (
        df
        .select(F.col("fetched_at"), F.explode(F.col("data")).alias("item"))
        .select(F.col("fetched_at"), F.col("item.*"))
    )

    return (
        items
        .groupBy("item_id")
        .agg(
            F.sum("wins").alias("total_wins"),
            F.sum("losses").alias("total_losses"),
            F.sum("matches").alias("total_matches"),
            F.max("fetched_at").alias("last_updated"),
        )
        .withColumn(
            "win_rate",
            F.round(F.col("total_wins") / F.col("total_matches"), 4),
        )
        .filter(F.col("total_matches") > 0)
    )


def process_generic(df):
    """Explode data array + flatten structs. Works for most endpoints."""
    if "data" not in df.columns:
        print("    WARN: no 'data' column found")
        return None

    rows = (
        df
        .select(F.col("fetched_at"), F.explode(F.col("data")).alias("record"))
        .select(F.col("fetched_at"), F.col("record.*"))
    )
    return flatten(rows)


def process_shallow(df):
    """Explode data array but DON'T flatten structs.
    For big endpoints (match_metadata, match_history) — flattening those
    caused OOM and mongo handles nested docs fine anyway."""
    if "data" not in df.columns:
        print("    WARN: no 'data' column found")
        return None

    return (
        df
        .select(F.col("fetched_at"), F.explode(F.col("data")).alias("record"))
        .select(F.col("fetched_at"), F.col("record.*"))
    )


# --- Main loop ---

def main():
    start = datetime.now(timezone.utc)
    print(f"[{start.isoformat()}] Starting Spark processing run...")
    print(f"  Endpoints to process: {len(ENDPOINTS)}\n")

    ok = 0
    skip = 0
    fail = 0

    for endpoint, mode in ENDPOINTS.items():
        print(f"[{endpoint}]")

        df = read_endpoint(endpoint)
        if df is None:
            skip += 1
            continue

        try:
            if mode == "hero":
                result = process_hero(df)
            elif mode == "item":
                result = process_item(df)
            elif mode == "shallow":
                result = process_shallow(df)
            else:
                result = process_generic(df)

            if result is None:
                print(f"  SKIP\t{endpoint} — processing returned nothing")
                skip += 1
                continue

            count = result.count()
            write_collection(result, endpoint)
            print(f"  OK\t{endpoint}: {count} rows -> MongoDB '{endpoint}'")
            ok += 1

        except Exception as e:
            print(f"  FAIL\t{endpoint}: {e}", file=sys.stderr)
            fail += 1

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    total = ok + skip + fail
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Done.")
    print(f"  {ok} written, {skip} skipped, {fail} failed  "
          f"({total} total) in {elapsed:.1f}s")

    spark.stop()
    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
