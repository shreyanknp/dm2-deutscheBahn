from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------------------
# Configurations
# ----------------------------
BQ_PROJECT = "dm2-nov-25"           # <-- replace with your project
BQ_DATASET = "fchg_data"               # <-- replace with your dataset
TEMP_BUCKET = "my-bq-staging-bucket"        # <-- replace with your staging GCS bucket

# Paths in GCS
FCHG_PATH = "gs://db-raw-data-dm2/raw/timetables/fchg/train_movement.csv"
DIM_TRAIN_PATH = "gs://db-raw-data-dm2/raw/reference/dimensions/train_plan.csv"
DIM_STATION_PATH = "gs://db-raw-data-dm2/raw/reference/dimensions/stations.csv"

# ----------------------------
# Initialize Spark session
# ----------------------------
spark = SparkSession.builder \
    .appName("TrainMovement_DimTrain_Station_to_BQ") \
    .getOrCreate()

# ----------------------------
# 1️⃣ Process FCHG / train movement
# ----------------------------
fchg_df = spark.read.option("header", True).csv(FCHG_PATH)

# Only cast numeric columns that actually exist
fchg_df = fchg_df.withColumn("delay_minutes", fchg_df["delay_minutes"].cast("int"))

fchg_df.write \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.train_movement") \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save()

# ----------------------------
# 2️⃣ Process dim_train
# ----------------------------
dim_train_df = spark.read.option("header", True).csv(DIM_TRAIN_PATH)

dim_train_df.write \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.train_plan") \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save()

# ----------------------------
# 3️⃣ Process dim_station
# ----------------------------
dim_station_df = spark.read.option("header", True).csv(DIM_STATION_PATH)

dim_station_df.write \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.stations") \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save()

# ----------------------------
# 4️⃣ Process population data
# ----------------------------
POPULATION_PATH = "gs://db-raw-data-dm2/static/deutschland_cities_2024.csv"

population_df = spark.read.option("header", True).csv(POPULATION_PATH)

# Cast numeric columns if necessary
if "population" in population_df.columns:
    population_df = population_df.withColumn("population", population_df["population"].cast("int"))

population_df.write \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.population") \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .mode("overwrite") \
    .save()

# ----------------------------
# Stop Spark session
# ----------------------------
spark.stop()