import os
import requests
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from google.cloud import storage
from zoneinfo import ZoneInfo

def extract_db_data(request):
    # ---------------- CONFIG ---------------- #
    BASE_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"

    CLIENT_ID = os.environ["DB_CLIENT_ID"]
    API_KEY = os.environ["DB_API_KEY"]
    GCS_BUCKET = os.environ["GCS_BUCKET_NAME"]

    HEADERS = {
        "DB-Client-Id": CLIENT_ID,
        "DB-Api-Key": API_KEY,
        "Accept": "application/xml"
    }

    STATION_PATTERNS = [
        "Berlin Hbf", "Mannheim Hbf", "München Hbf", "Hamburg Hbf",
        "Frankfurt Hbf", "Stuttgart Hbf", "Köln Hbf",
        "Dresden Hbf", "Leipzig Hbf", "Heidelberg Hbf"
    ]

    MAIN_STATION_EVA = "8000244"  # Mannheim Hbf
    STATION_SLUG = "mannheim"

    # -------- TIME PARTITIONS (GERMANY TIME) -------- #
    germany_time = datetime.now(ZoneInfo("Europe/Berlin"))

    DATE_API = germany_time.strftime("%y%m%d")
    HOUR_API = germany_time.strftime("%H")

    PARTITION_DATE = germany_time.strftime("%Y-%m-%d")
    PARTITION_HOUR = germany_time.strftime("%H")
    RUN_TS = germany_time.strftime("%Y%m%d_%H%M%S")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    # ==========================================================
    # DIM STATION (STATIC DIMENSION)
    # ==========================================================
    stations = {}

    for pattern in STATION_PATTERNS:
        url = f"{BASE_URL}/station/{pattern}"
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            continue

        root = ET.fromstring(r.text)
        for s in root.findall("station"):
            eva = s.attrib.get("eva")
            if eva and eva not in stations:
                stations[eva] = {
                    "station_id": eva,
                    "station_name": s.attrib.get("name"),
                    "ds100": s.attrib.get("ds100")
                }

        if len(stations) >= 10:
            break

    station_path = "/tmp/dim_station.csv"
    with open(station_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["station_id", "station_name", "ds100"]
        )
        writer.writeheader()
        writer.writerows(stations.values())

    bucket.blob(
        "raw/reference/dimensions/stations.csv"
    ).upload_from_filename(station_path)

    # ==========================================================
    # DIM TRAIN (SLOWLY CHANGING, SAFE TO OVERWRITE)
    # ==========================================================
    train_set = {}

    plan_url = f"{BASE_URL}/plan/{MAIN_STATION_EVA}/{DATE_API}/{HOUR_API}"
    plan_response = requests.get(plan_url, headers=HEADERS, timeout=20)
    plan_response.raise_for_status()

    root = ET.fromstring(plan_response.text)

    for stop in root.findall("s"):
        tl = stop.find("tl")
        if tl is None:
            continue

        train_type = tl.attrib.get("c")
        train_number = tl.attrib.get("n")
        if not train_type or not train_number:
            continue

        train_id = f"{train_type}_{train_number}"

        train_set[train_id] = {
            "train_id": train_id,
            "train_type": train_type,
            "train_number": train_number,
            "line_code": tl.attrib.get("l"),
            "operator_code": tl.attrib.get("o"),
            "traffic_type": tl.attrib.get("f"),
            "train_class": (
                "High Speed" if train_type == "ICE"
                else "Regional" if train_type in ["RE", "RB"]
                else "Urban"
            ),
            "first_seen_at": germany_time.isoformat()
        }

    train_path = "/tmp/dim_train.csv"
    with open(train_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=train_set[next(iter(train_set))].keys())
        writer.writeheader()
        writer.writerows(train_set.values())

    bucket.blob(
        "raw/reference/dimensions/train_plan.csv"
    ).upload_from_filename(train_path)

    # ==========================================================
    # FACT TRAIN MOVEMENT (fchg) — PARTITIONED
    # ==========================================================
    fact_rows = []

    fchg_url = f"{BASE_URL}/fchg/{MAIN_STATION_EVA}"
    r = requests.get(fchg_url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    for stop in root.findall("s"):
        train_elem = stop.find("tl")
        if train_elem is None:
            continue

        train_type = train_elem.attrib.get("c")
        train_number = train_elem.attrib.get("n")
        train_id = f"{train_type}_{train_number}"

        for ev_type in ["ar", "dp"]:
            ev = stop.find(ev_type)
            if ev is None:
                continue

            scheduled_time = ev.attrib.get("pt")
            actual_time = ev.attrib.get("ct")

            delay_minutes = None
            if scheduled_time and actual_time:
                delay_minutes = (
                    datetime.strptime(actual_time, "%y%m%d%H%M") -
                    datetime.strptime(scheduled_time, "%y%m%d%H%M")
                ).total_seconds() / 60

            fact_rows.append({
                "train_id": train_id,
                "station_id": MAIN_STATION_EVA,
                "event_type": ev_type,
                "scheduled_time": scheduled_time,
                "actual_time": actual_time,
                "delay_minutes": delay_minutes,
                "event_status": ev.attrib.get("cs"),
                "ingestion_time": germany_time.isoformat()
            })

    fact_path = "/tmp/fchg_train_movement.csv"
    with open(fact_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "train_id", "station_id", "event_type",
                "scheduled_time", "actual_time",
                "delay_minutes", "event_status", "ingestion_time"
            ]
        )
        writer.writeheader()
        writer.writerows(fact_rows)

    # gcs_fact_path = (
    #     f"raw/timetables/fchg/"
    #     f"date={PARTITION_DATE}/"
    #     f"hour={PARTITION_HOUR}/"
    #     f"fchg_{STATION_SLUG}_{RUN_TS}.csv"
    # )
    gcs_fact_path = "raw/timetables/fchg/train_movement.csv"
    bucket.blob(gcs_fact_path).upload_from_filename(fact_path)

    from google.cloud import dataproc_v1

    PROJECT_ID = "dm2-nov-25"
    REGION = "europe-west1"
    CLUSTER_NAME = "db-spark-cluster-m"
    SPARK_PY_FILE = "gs://db-raw-data-dm2/spark/spark_transform.py"

    def trigger_spark_job(event, context):
        """Cloud Function to trigger the existing Dataproc Spark job."""
        client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com"})

        job = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": SPARK_PY_FILE
            }
        }

        operation = client.submit_job_as_operation(project_id=PROJECT_ID, region=REGION, job=job)
        response = operation.result()  # Wait for job completion
        print("Dataproc Spark job finished:", response.reference.job_id)


    return {
        "status": "success",
        "stations": len(stations),
        "trains": len(train_set),
        "events": len(fact_rows),
        "gcs_path": gcs_fact_path
    }


