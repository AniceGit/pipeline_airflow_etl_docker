from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os, requests, zipfile, duckdb, pandas as pd
from google.transit import gtfs_realtime_pb2
import uuid
import pytz
import shutil

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
    os.makedirs("/opt/airflow/warehouse", exist_ok=True)
    con = duckdb.connect(WAREHOUSE)
    
    for fname in ["stops", "routes", "trips", "stop_times", "shapes", "feed_info", "calendar", "calendar_dates", "agency"]:
        path = f"{DATA}/static/{fname}.txt"
        con.execute(f"CREATE OR REPLACE TABLE {fname} AS SELECT * FROM read_csv_auto('{path}', ALL_VARCHAR=TRUE)")
    con.close()

def init_schema():
    con = duckdb.connect(WAREHOUSE)
    # Supprimer proprement en cascade
    con.execute("DROP TABLE IF EXISTS Fact_Event CASCADE")
    con.execute("DROP TABLE IF EXISTS Dim_stop CASCADE")
    con.execute("DROP TABLE IF EXISTS Dim_trip CASCADE")
    con.execute("DROP TABLE IF EXISTS Dim_route CASCADE")
    con.execute("DROP TABLE IF EXISTS Dim_time CASCADE")

    con.execute("""
        CREATE OR REPLACE TABLE Dim_stop (
            stop_id VARCHAR PRIMARY KEY,
            stop_name VARCHAR,
            stop_lat DOUBLE,
            stop_lon DOUBLE
        )
    """)
    con.execute("""
        CREATE OR REPLACE TABLE Dim_trip (
            trip_id VARCHAR PRIMARY KEY,
            trip_headsign VARCHAR,
            direction_id INTEGER
        )
    """)
    con.execute("""
        CREATE OR REPLACE TABLE Dim_route (
            route_id VARCHAR PRIMARY KEY,
            route_type INTEGER,
            route_short_name VARCHAR,
            route_long_name VARCHAR,
            route_color VARCHAR
        )
    """)
    con.execute("""
        CREATE OR REPLACE TABLE Fact_Event (
            event_id VARCHAR PRIMARY KEY,
            event_ts TIMESTAMP,
            trip_id VARCHAR REFERENCES Dim_trip(trip_id),
            route_id VARCHAR REFERENCES Dim_route(route_id),
            stop_id VARCHAR REFERENCES Dim_stop(stop_id),
            vehicle_id VARCHAR,
            arrival_time VARCHAR,
            departure_time VARCHAR,
            planned_arrival DOUBLE,
            planned_departure DOUBLE,
            arrival_time_rt DOUBLE,
            departure_time_rt DOUBLE,
            delay_min DOUBLE,
            on_time INTEGER,
            lat DOUBLE,
            lon DOUBLE
        )
    """)
    con.execute("""
        CREATE OR REPLACE TABLE Dim_time (
            time_id VARCHAR PRIMARY KEY,
            event_ts TIMESTAMP,
            date DATE,
            hour INTEGER,
            minute INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            day INTEGER
        )
    """)
    con.close()


def build_fact_event():
    os.makedirs("/opt/airflow/warehouse", exist_ok=True)
    con = duckdb.connect(WAREHOUSE)

    #Charger les tables statiques
    stops = con.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops").df()
    #trips = con.execute("SELECT trip_id, trip_headsign, direction_id FROM trips").df()
    trips = con.execute("SELECT trip_id, route_id, trip_headsign, direction_id FROM trips").df()
    routes = con.execute("SELECT route_id, route_type, route_short_name, route_long_name, route_color FROM routes").df()
    #Horaires officiels : arrival_time et departure_time = chaîne "HH:MM:SS"
    stop_times = con.execute("SELECT trip_id, stop_id, arrival_time, departure_time FROM stop_times").df()

    #---Conversion des heures prévues en timestamp Unix---
    LOCAL_TZ = pytz.timezone("Europe/Paris")
    def arrival_to_unix(t: str, service_date: datetime.date):
        if pd.isna(t): 
            return None
        try:
            h, m, s = map(int, t.split(":"))
        except Exception:
            return None
        dt = datetime(service_date.year, service_date.month, service_date.day, h % 24, m, s)
        if h >= 24:  # gérer GTFS > 24h
            dt = dt + timedelta(days=1)
        # localiser en Europe/Paris puis convertir en UTC
        dt = LOCAL_TZ.localize(dt).astimezone(timezone.utc)
        return dt.timestamp()

    #---Récupérer la date du feed realtime---
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(open(f"{DATA}/trip_updates.pb", "rb").read())
    service_date = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc).date()

    #---Conversion des horaires officiels avec cette date en UNIX timestamp | Exemple : "16:02:00" → 1.757434e+09---
    stop_times["planned_arrival"] = stop_times["arrival_time"].apply(lambda t: arrival_to_unix(t, service_date))
    stop_times["planned_departure"] = stop_times["departure_time"].apply(lambda t: arrival_to_unix(t, service_date))

    #---TRIP UPDATES (Realtime)---
    # feed = gtfs_realtime_pb2.FeedMessage()
    # feed.ParseFromString(open(f"{DATA}/trip_updates.pb", "rb").read())
    rt_rows = []
    for e in feed.entity:
        if e.HasField("trip_update"):
            trip = e.trip_update.trip.trip_id
            for stu in e.trip_update.stop_time_update:
                rt_rows.append({
                    "trip_id": trip,
                    "stop_id": stu.stop_id,
                    #arrival_time_rt et departure_time_rt sont des UNIX timestamp
                    "arrival_time_rt": stu.arrival.time if stu.HasField("arrival") else None,
                    "departure_time_rt": stu.departure.time if stu.HasField("departure") else None
                })
    df_rt = pd.DataFrame(rt_rows)

    #---VEHICLE POSITIONS---
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
                "lon": v.position.longitude if v.HasField("position") else None,
                "vehicle_ts": v.timestamp if v.HasField("timestamp") else None
            })
    df_vehicle = pd.DataFrame(vrows)

    #---JOIN Static + RT---
    fact = (
        df_rt.merge(stop_times, on=["trip_id", "stop_id"], how="left")
             .merge(trips, on="trip_id", how="left")
             .merge(routes, on="route_id", how="left")
             .merge(df_vehicle, on="trip_id", how="left")
    )
    #VERIFICATION :
    print("DEBUT TEST DE LA TABLE FACT--------------------------")
    print(fact[["trip_id","stop_id","planned_arrival","arrival_time_rt"]].head(10))
    print("len :", len(fact), len(df_rt), len(stop_times))
    print("FIN TEST DE LA TABLE FACT--------------------------")

    #---Calcul du retard---
    fact["arrival_time_rt"] = pd.to_numeric(fact["arrival_time_rt"], errors="coerce")
    # Nettoyage : remplacer 0 par NaN (valeur manquante)
    fact.loc[fact["arrival_time_rt"] == 0, "arrival_time_rt"] = pd.NA

    fact["delay_min"] = (fact["arrival_time_rt"] - fact["planned_arrival"]) / 60
    fact["delay_min"] = fact["delay_min"].where(fact["arrival_time_rt"].notna())
    fact["on_time"] = fact["delay_min"].apply(lambda d: 1 if pd.notna(d) and d <= 5 else 0)

    print("______________________TEST__________________________")
    print("Delay sample:", fact[["planned_arrival","arrival_time_rt","delay_min"]].head(10))
    print("____________________END TEST________________________")

    #---Colonnes finales---
    fact_event = pd.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(len(fact))],
        "event_ts": pd.to_datetime(fact["vehicle_ts"], unit="s", utc=True),
        "trip_id": fact["trip_id"],
        "route_id": fact["route_id"],
        "stop_id": fact["stop_id"],
        "vehicle_id": fact["vehicle_id"],
        "arrival_time": fact["arrival_time"],
        "departure_time": fact["departure_time"],
        "planned_arrival": fact["planned_arrival"], #float UNIX timestamp
        "planned_departure": fact["planned_departure"],
        "arrival_time_rt": fact["arrival_time_rt"], #float UNIX timestamp
        "departure_time_rt": fact["departure_time_rt"],
        "delay_min": fact["delay_min"], #float minutes
        "on_time": fact["on_time"],
        "lat": fact["lat"],
        "lon": fact["lon"]
    })

    # #Sauvegarde dans DuckDB
    # con.register("df", fact_event)
    # con.execute("CREATE OR REPLACE TABLE Fact_Event AS SELECT * FROM df")

    # #Dimensions
    # con.execute("CREATE OR REPLACE TABLE Dim_stop AS SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon FROM stops")
    # con.execute("CREATE OR REPLACE TABLE Dim_trip AS SELECT DISTINCT trip_id, trip_headsign, direction_id FROM trips")
    # con.execute("CREATE OR REPLACE TABLE Dim_route AS SELECT DISTINCT route_id, route_type, route_short_name, route_long_name, route_color FROM routes")

    # --- Insertions (plus de CREATE seulement INSERT) ---
    con.register("df_stops", stops)
    con.execute("INSERT OR REPLACE INTO Dim_stop SELECT * FROM df_stops")

    trips = trips.drop(columns=["route_id"])
    con.register("df_trips", trips)
    con.execute("INSERT OR REPLACE INTO Dim_trip SELECT * FROM df_trips")

    con.register("df_routes", routes)
    con.execute("INSERT OR REPLACE INTO Dim_route SELECT * FROM df_routes")

    con.register("df_fact", fact_event)
    con.execute("INSERT INTO Fact_Event SELECT * FROM df_fact")

    #Dimension temps
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
    con.execute("INSERT INTO Dim_time SELECT * FROM df_time")

    con.close()



def load_exports():
    # ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    # outdir = f"{EXPORTS}/{ts}"
    # os.makedirs(outdir, exist_ok=True)
    outdir = f"{EXPORTS}/latest"
    # Supprime tout ce qui est dans le dossier s'il existe
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)


    con = duckdb.connect(WAREHOUSE)

    #Exporter le modèle en étoile (Fact + Dim)
    for table in ["Fact_Event", "Dim_stop", "Dim_trip", "Dim_route", "Dim_time"]:
        con.execute(f"COPY (SELECT * FROM {table}) TO '{outdir}/{table}.parquet' (FORMAT PARQUET)")

    #---KPI---

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
        SELECT 
            EXTRACT(hour FROM d.event_ts AT TIME ZONE 'Europe/Paris') AS local_hour,
            AVG(f.delay_min) AS avg_delay
        FROM Fact_Event f
        JOIN Dim_time d ON f.event_ts = d.event_ts
        WHERE f.delay_min IS NOT NULL
        GROUP BY local_hour
        ORDER BY local_hour
    """).df()
    df_kpi_by_hour.to_parquet(f"{outdir}/kpi_delay_by_hour.parquet", index=False)

    con.close()

with DAG(
    "gtfs_duckdb",
    default_args=default_args,
    schedule="*/15 * * * *",  # toutes les 15 minutes
    start_date=datetime(2025,1,1),
    catchup=False,
    max_active_tasks=1, #1 tâche à la fois
) as dag:

    t1 = PythonOperator(task_id="extract_static", python_callable=extract_static)
    t2 = PythonOperator(task_id="extract_rt_tripupdates", python_callable=extract_rt_tripupdates)
    t3 = PythonOperator(task_id="extract_rt_vehiclepos", python_callable=extract_rt_vehiclepos)
    t4 = PythonOperator(task_id="transform_static", python_callable=transform_static)
    t5 = PythonOperator(task_id="init_schema", python_callable=init_schema)
    t6 = PythonOperator(task_id="build_fact_event", python_callable=build_fact_event)
    t7 = PythonOperator(task_id="load_exports", python_callable=load_exports)

    [t1, t2, t3] >> t4 >> t5 >> t6 >> t7

