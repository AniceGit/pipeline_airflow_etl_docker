from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os, requests, zipfile, duckdb, pandas as pd
from google.transit import gtfs_realtime_pb2
import uuid

BASE = "/opt/airflow"
DATA = f"{BASE}/data"
WAREHOUSE = f"{BASE}/warehouse/warehouse.duckdb"
EXPORTS = f"{BASE}/exports"

URL_STATIC = "https://www.data.gouv.fr/api/1/datasets/r/f5678ab2-c863-4b48-ba1f-9021c7d97634"
URL_TRIPUP = "https://www.data.gouv.fr/api/1/datasets/r/af3f0734-ef07-468e-b8c9-aed97e4c8a32"
URL_VEHICLE = "https://www.data.gouv.fr/api/1/datasets/r/5f571595-aef1-480f-acde-b9315d9f5f3b"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

def download_file(url, out):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with open(out, "wb") as f:
        f.write(r.content)

def extract_static():
    os.makedirs(DATA, exist_ok=True)
    zip_path = f"{DATA}/static_gtfs.zip"
    download_file(URL_STATIC, zip_path)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(f"{DATA}/static")

def extract_rt_tripupdates():
    download_file(URL_TRIPUP, f"{DATA}/trip_updates.pb")

def extract_rt_vehiclepos():
    download_file(URL_VEHICLE, f"{DATA}/vehicle_pos.pb")

def transform_static():
    con = duckdb.connect(WAREHOUSE)
    for fname in ["stops", "routes", "trips", "stop_times", "shapes", "feed_info", "calendar", "calendar_dates", "agency"]:
        path = f"{DATA}/static/{fname}.txt"
        con.execute(f"CREATE OR REPLACE TABLE {fname} AS SELECT * FROM read_csv_auto('{path}', ALL_VARCHAR=TRUE)")
    con.close()

def build_fact_event():
    con = duckdb.connect(WAREHOUSE)

    # Charger les tables statiques
    stops = con.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops").df()
    trips = con.execute("SELECT trip_id, route_id, trip_headsign, direction_id FROM trips").df()
    routes = con.execute("SELECT route_id, route_type, route_short_name, route_long_name, route_color FROM routes").df()
    stop_times = con.execute("SELECT trip_id, stop_id, arrival_time, departure_time FROM stop_times").df()

    # Convertir les heures prévues en secondes depuis minuit
    def to_sec(t):
        if pd.isna(t): return None
        h, m, s = map(int, t.split(":"))
        return h*3600 + m*60 + s
    stop_times["arrival_sec"] = stop_times["arrival_time"].apply(to_sec)
    stop_times["departure_sec"] = stop_times["departure_time"].apply(to_sec)

    # --- TRIP UPDATES (Realtime) ---
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(open(f"{DATA}/trip_updates.pb", "rb").read())
    rt_rows = []
    for e in feed.entity:
        if e.HasField("trip_update"):
            trip = e.trip_update.trip.trip_id
            for stu in e.trip_update.stop_time_update:
                rt_rows.append({
                    "trip_id": trip,
                    "stop_id": stu.stop_id,
                    "arrival_time_rt": stu.arrival.time if stu.HasField("arrival") else None,
                    "departure_time_rt": stu.departure.time if stu.HasField("departure") else None
                })
    df_rt = pd.DataFrame(rt_rows)

    # --- VEHICLE POSITIONS ---
    feed2 = gtfs_realtime_pb2.FeedMessage()
    feed2.ParseFromString(open(f"{DATA}/vehicle_pos.pb", "rb").read())
    vrows = []
    for e in feed2.entity:
        if e.HasField("vehicle"):
            v = e.vehicle
            vrows.append({
                "trip_id": v.trip.trip_id,
                "vehicle_id": v.vehicle.id if v.vehicle.id else None,
                "lat": v.position.latitude if v.HasField("position") else None,
                "lon": v.position.longitude if v.HasField("position") else None
            })
    df_vehicle = pd.DataFrame(vrows)

    # --- JOIN Static + RT ---
    fact = (
        df_rt.merge(stop_times, on=["trip_id", "stop_id"], how="left")
             .merge(trips, on="trip_id", how="left")
             .merge(routes, on="route_id", how="left")
             .merge(df_vehicle, on="trip_id", how="left")
    )

    # Calcul du retard
    today_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    fact["planned_arrival"] = today_midnight + fact["arrival_sec"].fillna(0)
    fact["delay_min"] = (fact["arrival_time_rt"] - fact["planned_arrival"]) / 60
    fact["on_time"] = fact["delay_min"].apply(lambda d: 1 if d is not None and d <= 5 else 0)  # ex seuil 5min

    # Ajouter colonnes finales
    fact_event = pd.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(len(fact))],
        "event_ts": datetime.now(timezone.utc),
        "trip_id": fact["trip_id"],
        "route_id": fact["route_id"],
        "stop_id": fact["stop_id"],
        "vehicle_id": fact["vehicle_id"],
        "arrival_time": fact["arrival_time"],
        "departure_time": fact["departure_time"],
        "arrival_time_rt": fact["arrival_time_rt"],
        "departure_time_rt": fact["departure_time_rt"],
        "delay_min": fact["delay_min"],
        "on_time": fact["on_time"],
        "lat": fact["lat"],
        "lon": fact["lon"]
    })

    # Attacher le DataFrame Pandas comme table temporaire "df"
    con.register("df", fact_event)
    # Sauvegarder dans DuckDB en utilisant la table temporaire df
    con.execute("CREATE OR REPLACE TABLE Fact_Event AS SELECT * FROM df")

    # Créer/MAJ les dimensions
    con.execute("CREATE OR REPLACE TABLE Dim_stop AS SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon FROM stops")
    con.execute("CREATE OR REPLACE TABLE Dim_trip AS SELECT DISTINCT trip_id, trip_headsign, direction_id FROM trips")
    con.execute("CREATE OR REPLACE TABLE Dim_route AS SELECT DISTINCT route_id, route_type, route_short_name, route_long_name, route_color FROM routes")

    # Dimension temps
    fact_event["date"] = pd.to_datetime(fact_event["event_ts"]).dt.date
    fact_event["hour"] = pd.to_datetime(fact_event["event_ts"]).dt.hour
    fact_event["minute"] = pd.to_datetime(fact_event["event_ts"]).dt.minute
    fact_event["week"] = pd.to_datetime(fact_event["event_ts"]).dt.isocalendar().week
    fact_event["month"] = pd.to_datetime(fact_event["event_ts"]).dt.month
    fact_event["year"] = pd.to_datetime(fact_event["event_ts"]).dt.year
    fact_event["day"] = pd.to_datetime(fact_event["event_ts"]).dt.day

    dim_time = pd.DataFrame({
        "time_id": [str(uuid.uuid4()) for _ in range(len(fact_event))],
        "event_ts": fact_event["event_ts"],
        "date": fact_event["date"],
        "hour": fact_event["hour"],
        "minute": fact_event["minute"],
        "week": fact_event["week"],
        "month": fact_event["month"],
        "year": fact_event["year"],
        "day": fact_event["day"]
    })
    con.register("df_time", dim_time)
    con.execute("CREATE OR REPLACE TABLE Dim_time AS SELECT * FROM df_time")

    con.close()


def load_exports():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    outdir = f"{EXPORTS}/{ts}"
    os.makedirs(outdir, exist_ok=True)

    con = duckdb.connect(WAREHOUSE)

    # Exporter le modèle en étoile (Fact + Dim)
    for table in ["Fact_Event", "Dim_stop", "Dim_trip", "Dim_route", "Dim_time"]:
        con.execute(f"COPY (SELECT * FROM {table}) TO '{outdir}/{table}.parquet' (FORMAT PARQUET)")

    # --- Exemples de KPI calculés à la volée ---

    #1. Retard moyen global (KPI 1)
    df_kpi_delay = con.execute("""
        SELECT AVG(delay_min) AS avg_delay, COUNT(*) AS n_events
        FROM Fact_Event
    """).df()
    df_kpi_delay.to_parquet(f"{outdir}/kpi_avg_delay.parquet", index=False)

    #2. Taux de ponctualité par ligne (<= 5 min de retard) (KPI 6)
    df_kpi_ontime = con.execute("""
        SELECT route_id, 
               100.0 * SUM(on_time) / COUNT(*) AS pct_on_time
        FROM Fact_Event
        GROUP BY route_id
    """).df()
    df_kpi_ontime.to_parquet(f"{outdir}/kpi_ontime_route.parquet", index=False)

    #3. Moyenne des retards par heure de la journée (KPI 5)
    df_kpi_by_hour = con.execute("""
        SELECT d.hour, AVG(f.delay_min) AS avg_delay
        FROM Fact_Event f
        JOIN Dim_time d ON f.event_ts = d.event_ts
        GROUP BY d.hour
        ORDER BY d.hour
    """).df()
    df_kpi_by_hour.to_parquet(f"{outdir}/kpi_delay_by_hour.parquet", index=False)

    con.close()

with DAG(
    "gtfs_duckdb",
    default_args=default_args,
    schedule="*/15 * * * *",  # toutes les 15 minutes
    start_date=datetime(2025,1,1),
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract_static", python_callable=extract_static)
    t2 = PythonOperator(task_id="extract_rt_tripupdates", python_callable=extract_rt_tripupdates)
    t3 = PythonOperator(task_id="extract_rt_vehiclepos", python_callable=extract_rt_vehiclepos)
    t4 = PythonOperator(task_id="transform_static", python_callable=transform_static)
    t5 = PythonOperator(task_id="build_fact_event", python_callable=build_fact_event)
    t6 = PythonOperator(task_id="load_exports", python_callable=load_exports)

    [t1, t2, t3] >> t4 >> t5 >> t6

