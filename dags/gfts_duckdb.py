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
    """
    Télécharge un fichier depuis une URL et l'enregistre localement.

    Parameters
    ----------
    url : str
        URL du fichier à télécharger.
    out : str
        Chemin de sortie où enregistrer le fichier.
    """
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with open(out, "wb") as f:
        f.write(r.content)

def extract_static():
    """
    Télécharge et extrait les fichiers GTFS statiques (format zip) dans le dossier `data/static`.
    """
    os.makedirs(DATA, exist_ok=True)
    zip_path = f"{DATA}/static_gtfs.zip"
    download_file(URL_STATIC, zip_path)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(f"{DATA}/static")

def extract_rt_tripupdates():
    """
    Télécharge les mises à jour temps réel des trajets (trip updates) au format Protocol Buffer.
    """
    os.makedirs(DATA, exist_ok=True)
    download_file(URL_TRIPUP, f"{DATA}/trip_updates.pb")

def extract_rt_vehiclepos():
    """
    Télécharge les positions temps réel des véhicules (vehicle positions) au format Protocol Buffer.
    """
    os.makedirs(DATA, exist_ok=True)
    download_file(URL_VEHICLE, f"{DATA}/vehicle_pos.pb")

def transform_static():
    """
    Transforme les fichiers GTFS statiques (TXT) en tables DuckDB, en les important dans le warehouse.
    Toutes les colonnes sont importées en tant que chaînes (ALL_VARCHAR).
    """
    os.makedirs(os.path.dirname(WAREHOUSE), exist_ok=True)
    with duckdb.connect(WAREHOUSE) as con:
        for fname in ["stops", "routes", "trips", "stop_times", "shapes", "feed_info", "calendar", "calendar_dates", "agency"]:
            path = f"{DATA}/static/{fname}.txt"
            con.execute(f"CREATE OR REPLACE TABLE {fname} AS SELECT * FROM read_csv_auto('{path}', ALL_VARCHAR=TRUE)")

def init_schema():
    """
    Initialise le schéma du data warehouse DuckDB :
    - Crée les dimensions (stop, trip, route, time)
    - Crée la table de faits (Fact_Event) si elles n'existent pas.
    """
    with duckdb.connect(WAREHOUSE) as con:
        # con.execute("DROP TABLE IF EXISTS Fact_Event CASCADE")
        # con.execute("DROP TABLE IF EXISTS Dim_stop CASCADE")
        # con.execute("DROP TABLE IF EXISTS Dim_trip CASCADE")
        # con.execute("DROP TABLE IF EXISTS Dim_route CASCADE")
        # con.execute("DROP TABLE IF EXISTS Dim_time CASCADE")

        #CREATE OR REPLACE TABLE???
        con.execute("""
            CREATE TABLE IF NOT EXISTS Dim_stop (
                stop_id VARCHAR PRIMARY KEY,
                stop_name VARCHAR,
                stop_lat DOUBLE,
                stop_lon DOUBLE
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS Dim_trip (
                trip_id VARCHAR PRIMARY KEY,
                trip_headsign VARCHAR,
                direction_id INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS Dim_route (
                route_id VARCHAR PRIMARY KEY,
                route_type INTEGER,
                route_short_name VARCHAR,
                route_long_name VARCHAR,
                route_color VARCHAR
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS Fact_Event (
                event_id VARCHAR PRIMARY KEY,
                event_ts TIMESTAMP,
                trip_id VARCHAR REFERENCES Dim_trip(trip_id),
                route_id VARCHAR REFERENCES Dim_route(route_id),
                stop_id VARCHAR REFERENCES Dim_stop(stop_id),
                vehicle_id VARCHAR,
                arrival_time VARCHAR,
                departure_time VARCHAR,
                planned_arrival TIMESTAMP,
                planned_departure TIMESTAMP,
                arrival_time_rt TIMESTAMP,
                departure_time_rt TIMESTAMP,
                delay_min DOUBLE,
                on_time INTEGER,
                lat DOUBLE,
                lon DOUBLE
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS Dim_time (
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

def build_fact_event():
    """
    Construit la table de faits `Fact_Event` en combinant :
    - Les données statiques (trips, stops, routes, stop_times)
    - Les flux temps réel (trip updates et vehicle positions)
    
    Calcule :
    - Les horaires planifiés (en timezone Europe/Paris)
    - Les horaires temps réel
    - Les retards en minutes
    - Le statut de ponctualité (`on_time`)
    
    Insère les dimensions et la table de faits dans DuckDB.
    Crée aussi la dimension temporelle (`Dim_time`) à partir de `event_ts`.
    """
    os.makedirs(os.path.dirname(WAREHOUSE), exist_ok=True)
    PARIS_TZ = pytz.timezone("Europe/Paris")

    with duckdb.connect(WAREHOUSE) as con:
        # Charger les tables statiques
        stops = con.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops").df()
        trips = con.execute("SELECT trip_id, route_id, trip_headsign, direction_id FROM trips").df()
        routes = con.execute("SELECT route_id, route_type, route_short_name, route_long_name, route_color FROM routes").df()
        stop_times = con.execute("SELECT trip_id, stop_id, arrival_time, departure_time FROM stop_times").df()

    # Fonction pour convertir HH:MM:SS + date service en timestamp France/Paris
    def arrival_to_paris_ts(t: str, service_date: datetime.date):
        if pd.isna(t):
            return pd.NaT
        h, m, s = map(int, t.split(":"))
        day = service_date
        if h >= 24:
            h = h % 24
            day += timedelta(days=1)
        dt = datetime(day.year, day.month, day.day, h, m, s)
        return PARIS_TZ.localize(dt)

    # Charger feed realtime
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(open(f"{DATA}/trip_updates.pb", "rb").read())
    service_date = datetime.fromtimestamp(feed.header.timestamp, tz=PARIS_TZ).date()

    # Ajouter colonnes planifiées avec timezone Paris
    stop_times["planned_arrival_paris"] = stop_times["arrival_time"].apply(lambda t: arrival_to_paris_ts(t, service_date))
    stop_times["planned_departure_paris"] = stop_times["departure_time"].apply(lambda t: arrival_to_paris_ts(t, service_date))

    # TRIP UPDATES
    rt_rows = []
    for e in feed.entity:
        if e.HasField("trip_update"):
            trip = e.trip_update.trip.trip_id
            for stu in e.trip_update.stop_time_update:
                arr_ts = pd.to_datetime(stu.arrival.time, unit="s", utc=True).tz_convert(PARIS_TZ) if stu.HasField("arrival") and stu.arrival.time > 0 else pd.NaT
                dep_ts = pd.to_datetime(stu.departure.time, unit="s", utc=True).tz_convert(PARIS_TZ) if stu.HasField("departure") and stu.departure.time > 0 else pd.NaT
                rt_rows.append({
                    "trip_id": trip,
                    "stop_id": stu.stop_id,
                    "arrival_time_rt_paris": arr_ts,
                    "departure_time_rt_paris": dep_ts
                })
    df_rt = pd.DataFrame(rt_rows)

    # VEHICLE POSITIONS
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
                "vehicle_ts": pd.to_datetime(v.timestamp, unit="s", utc=True).tz_convert(PARIS_TZ) if v.HasField("timestamp") and v.timestamp > 0 else pd.NaT
            })
    df_vehicle = pd.DataFrame(vrows)

    # JOIN static + RT + vehicle
    fact = (
        df_rt.merge(stop_times, on=["trip_id", "stop_id"], how="left")
             .merge(trips, on="trip_id", how="left")
             .merge(routes, on="route_id", how="left")
             .merge(df_vehicle, on="trip_id", how="left")
    )

    # Calcul du retard en minutes
    fact["delay_min"] = (fact["arrival_time_rt_paris"] - fact["planned_arrival_paris"]).dt.total_seconds() / 60
    fact.loc[fact["arrival_time_rt_paris"].isna(), "delay_min"] = pd.NA
    fact["on_time"] = fact["delay_min"].apply(lambda d: 1 if pd.notna(d) and d <= 5 else 0)

    # Construire DataFrame final pour DuckDB
    fact_event = pd.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(len(fact))],
        "event_ts": fact["vehicle_ts"].dt.tz_localize(None) if fact["vehicle_ts"].notna().any() else pd.NaT,
        "trip_id": fact["trip_id"],
        "route_id": fact["route_id"],
        "stop_id": fact["stop_id"],
        "vehicle_id": fact["vehicle_id"],
        "arrival_time": fact["arrival_time"],
        "departure_time": fact["departure_time"],
        "planned_arrival": fact["planned_arrival_paris"].dt.tz_localize(None),
        "planned_departure": fact["planned_departure_paris"].dt.tz_localize(None),
        "arrival_time_rt": fact["arrival_time_rt_paris"].dt.tz_localize(None),
        "departure_time_rt": fact["departure_time_rt_paris"].dt.tz_localize(None),
        "delay_min": fact["delay_min"],
        "on_time": fact["on_time"],
        "lat": fact["lat"],
        "lon": fact["lon"]
    })

    # Insertions DuckDB
    with duckdb.connect(WAREHOUSE) as con:
        con.register("df_stops", stops)
        con.execute("INSERT OR REPLACE INTO Dim_stop SELECT * FROM df_stops")

        con.register("df_trips", trips.drop(columns=["route_id"]))
        con.execute("INSERT OR REPLACE INTO Dim_trip SELECT * FROM df_trips")

        con.register("df_routes", routes)
        con.execute("INSERT OR REPLACE INTO Dim_route SELECT * FROM df_routes")

        con.register("df_fact", fact_event)
        con.execute("INSERT INTO Fact_Event SELECT * FROM df_fact")

        # Dimension temps
        fact_event["date"] = fact_event["event_ts"].dt.date
        fact_event["hour"] = fact_event["event_ts"].dt.hour
        fact_event["minute"] = fact_event["event_ts"].dt.minute
        fact_event["week"] = fact_event["event_ts"].dt.isocalendar().week
        fact_event["month"] = fact_event["event_ts"].dt.month
        fact_event["year"] = fact_event["event_ts"].dt.year
        fact_event["day"] = fact_event["event_ts"].dt.day

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

def load_exports():
    """
    Exporte les données et indicateurs clés de performance (KPI) au format Parquet dans `exports/latest/`.

    Exports :
    - Les tables `Fact_Event`, `Dim_stop`, `Dim_trip`, `Dim_route`, `Dim_time`
    - KPI 1 : Retards moyens par heure
    - KPI 4 : Pourcentage de ponctualité par ligne
    - KPI 5 : Heatmap des retards par heure/jour
    - KPI 6 : Taux global de ponctualité
    - KPI 7 : Évolution du retard par arrêt
    """
    outdir = f"{EXPORTS}/latest"
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)

    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        for table in ["Fact_Event", "Dim_stop", "Dim_trip", "Dim_route", "Dim_time"]:
            con.execute(f"COPY (SELECT * FROM {table}) TO '{outdir}/{table}.parquet' (FORMAT PARQUET)")
        
        # KPI 1. Retards moyens par heure de la journée, tout en SQL
        df_kpi_delay = con.execute("""
            WITH hours AS (
                SELECT range AS local_hour
                FROM range(0, 24)  -- Génère les heures 0 à 23
            )
            SELECT 
                h.local_hour,
                COALESCE(AVG(f.delay_min), 0) AS avg_delay,
                COALESCE(COUNT(f.event_id), 0) AS n_events
            FROM hours h
            LEFT JOIN Fact_Event f
                ON EXTRACT(hour FROM f.event_ts AT TIME ZONE 'Europe/Paris') = h.local_hour
                AND f.delay_min IS NOT NULL
                AND f.event_ts IS NOT NULL
            GROUP BY h.local_hour
            ORDER BY h.local_hour
        """).df()

        df_kpi_delay.to_parquet(f"{outdir}/kpi_avg_delay.parquet", index=False)
        #KPI 2 directement sur fichier streamlit avec le fichier parquet de la table fact_event
        #KPI 3 directement sur fichier streamlit (dimestop et event)
        # KPI 4. Retard moyen par ligne ou % de on_time 
        df_kpi_ontime = con.execute("""
            SELECT 
                r.route_id,
                COALESCE(100.0 * SUM(f.on_time) / NULLIF(COUNT(f.on_time),0), 0) AS pct_on_time
            FROM Dim_route r
            LEFT JOIN Fact_Event f
                ON r.route_id = f.route_id
            GROUP BY r.route_id
            ORDER BY r.route_id
        """).df()
        df_kpi_ontime.to_parquet(f"{outdir}/kpi_ontime_route.parquet", index=False)

        #KPI 5. Heatmap heures × jours
        df_kpi_by_hour = con.execute("""
            SELECT 
                EXTRACT(hour FROM d.event_ts AT TIME ZONE 'Europe/Paris') AS local_hour,
                EXTRACT(dow  FROM d.event_ts AT TIME ZONE 'Europe/Paris') AS dow,
                AVG(f.delay_min) AS avg_delay
            FROM Fact_Event f
            JOIN Dim_time d ON f.event_ts = d.event_ts
            WHERE f.delay_min IS NOT NULL
            GROUP BY local_hour, dow
            ORDER BY dow, local_hour
        """).df()
        df_kpi_by_hour.to_parquet(f"{outdir}/kpi_delay_by_hour.parquet", index=False)

        # KPI 6. Taux de ponctualité global
        df_kpi_global_ontime = con.execute("""
            SELECT
                SUM(CASE WHEN delay_min <= 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_on_time,
                COUNT(*) AS n_events
            FROM Fact_Event
            WHERE delay_min IS NOT NULL
        """).df()
        df_kpi_global_ontime.to_parquet(f"{outdir}/kpi_global_ontime.parquet", index=False)

        # KPI 7. Evolution du retard par arrêt (avec nom d’arrêt)
        df_kpi_delay_stop = con.execute("""
            SELECT 
                f.stop_id,
                s.stop_name,
                d.event_ts,
                AVG(f.delay_min) AS avg_delay
            FROM Fact_Event f
            JOIN Dim_time d ON f.event_ts = d.event_ts
            LEFT JOIN Dim_stop s ON f.stop_id = s.stop_id
            WHERE f.delay_min IS NOT NULL
            GROUP BY f.stop_id, s.stop_name, d.event_ts
            ORDER BY f.stop_id, d.event_ts
        """).df()
        df_kpi_delay_stop.to_parquet(f"{outdir}/kpi_delay_stop.parquet", index=False)

"""
DAG Airflow `gtfs_duckdb` : Extraction, transformation, et chargement des données GTFS (transport public)
en temps réel et statiques dans DuckDB, avec génération d'indicateurs pour visualisation via Streamlit.

Fréquence : Toutes les 15 minutes
Source : Données GTFS France (data.gouv.fr)
Destinations : DuckDB + fichiers Parquet (exports)
"""
with DAG(
    "gtfs_duckdb",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2025,1,1),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(task_id="extract_static", python_callable=extract_static)
    t2 = PythonOperator(task_id="extract_rt_tripupdates", python_callable=extract_rt_tripupdates)
    t3 = PythonOperator(task_id="extract_rt_vehiclepos", python_callable=extract_rt_vehiclepos)
    t4 = PythonOperator(task_id="transform_static", python_callable=transform_static)
    t5 = PythonOperator(task_id="init_schema", python_callable=init_schema)
    t6 = PythonOperator(task_id="build_fact_event", python_callable=build_fact_event)
    t7 = PythonOperator(task_id="load_exports", python_callable=load_exports)

    [t1, t2, t3] >> t4 >> t5 >> t6 >> t7
