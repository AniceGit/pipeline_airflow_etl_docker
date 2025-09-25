# streamlit_app.py
import streamlit as st
import pandas as pd
import pathlib
import os
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="Lignes d'Azur - KPI Retards")

BASE = os.getenv("BASE", "/opt/airflow")
EXPORT_DIR = os.getenv("EXPORT_DIR", f"{BASE}/exports/latest")

@st.cache_data(ttl=60)
def load_parquet(file_name):
    path = pathlib.Path(EXPORT_DIR) / file_name
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()

st.title("📊 KPI Retards — Lignes d'Azur")

# --- KPI 1. Retards moyens dans la journée (par heure) ---
df_avg_delay = load_parquet("kpi_avg_delay.parquet")
if not df_avg_delay.empty:
    st.subheader("Évolution du retard moyen par heure de la journée")

    # Afficher la courbe (axe X = heure, axe Y = retard moyen)
    st.line_chart(
        df_avg_delay.set_index("local_hour")[["avg_delay"]],
        height=400
    )

    # Ajouter quelques stats globales
    avg_global = df_avg_delay["avg_delay"].mean()
    total_events = int(df_avg_delay["n_events"].sum())
    st.metric("Retard moyen global (min)", f"{avg_global:.2f}")
    st.caption(f"Nombre total d'événements considérés : {total_events:,}")
else:
    st.warning("Fichier kpi_avg_delay.parquet introuvable.")

# --- KPI 2. Carte des bus en temps réel ---
st.subheader("🚌 Carte des bus en temps réel")

df_events = load_parquet("Fact_Event.parquet")

if df_events.empty or "lat" not in df_events.columns or "lon" not in df_events.columns:
    st.warning("Impossible d'afficher la carte : données GPS manquantes.")
else:
    # Nettoyer données manquantes
    df_events = df_events.dropna(subset=["lat", "lon", "delay_min"]).copy()

    # Catégoriser le retard pour couleur
    def delay_color(d):
        if d <= 0: 
            return "À l’heure"
        elif d <= 5:
            return "Retard modéré"
        else:
            return "Retard important"

    df_events["status"] = df_events["delay_min"].apply(delay_color)

    # Carte interactive avec Plotly
    fig = px.scatter_mapbox(
        df_events,
        lat="lat",
        lon="lon",
        color="status",
        size_max=10,
        zoom=11,
        mapbox_style="open-street-map",
        hover_data={
            "trip_id": True,
            "route_id": True,
            "stop_id": True,
            "delay_min": ":.2f"
        },
        title="Positions GPS des bus en temps réel"
    )

    st.plotly_chart(fig, use_container_width=True)

# --- KPI 3. Carte des arrêts avec état de service ---
st.subheader("🚏 Carte des arrêts avec état de service")

df_stops = load_parquet("Dim_stop.parquet")
df_events = load_parquet("Fact_Event.parquet")

if df_stops.empty or df_events.empty:
    st.warning("Impossible d'afficher la carte : données manquantes.")
else:
    # Retard moyen par arrêt
    df_delay_by_stop = (
        df_events.dropna(subset=["stop_id", "delay_min"])
                 .groupby("stop_id", as_index=False)
                 .agg(avg_delay=("delay_min", "mean"),
                      n_events=("delay_min", "count"))
    )

    # Fusion arrêts statiques + retard moyen
    df_map = df_stops.merge(df_delay_by_stop, on="stop_id", how="left")

    # Remplacer NaN par 0 (sinon erreur Plotly)
    df_map["n_events"] = df_map["n_events"].fillna(0).astype(int)

    # Catégorisation du retard
    def stop_status(d):
        if pd.isna(d):
            return "Pas de données"
        elif d <= 0:
            return "À l’heure"
        elif d <= 5:
            return "Retard modéré"
        else:
            return "Retard important"

    df_map["status"] = df_map["avg_delay"].apply(stop_status)

    # Carte interactive
    fig = px.scatter_mapbox(
        df_map,
        lat="stop_lat",
        lon="stop_lon",
        color="status",
        size="n_events",
        size_max=15,
        zoom=11,
        mapbox_style="open-street-map",
        hover_data={
            "stop_name": True,
            "avg_delay": ":.2f",
            "n_events": True
        },
        title="État de service des arrêts"
    )

    st.plotly_chart(fig, use_container_width=True)

# --- KPI 4. Taux de ponctualité par ligne (scroll horizontal) ---
st.subheader("🎯 Taux de ponctualité par ligne")
df_ontime = load_parquet("kpi_ontime_route.parquet")
df_routes = load_parquet("Dim_route.parquet")  # on récupère la liste complète des routes si dispo

if df_ontime.empty:
    st.warning("Fichier kpi_ontime_route.parquet introuvable.")
else:
    df_ontime = df_ontime.dropna(subset=["route_id"]).copy()
    df_ontime["route_id"] = df_ontime["route_id"].astype(str)

    if (not df_routes.empty) and ("route_id" in df_routes.columns):
        route_order = [str(x) for x in df_routes["route_id"].tolist()]
        df_all = pd.DataFrame({"route_id": route_order})
        df_ontime = df_all.merge(df_ontime, on="route_id", how="left")
        df_ontime["pct_on_time"] = df_ontime["pct_on_time"].fillna(0.0)
    else:
        df_ontime = df_ontime.groupby("route_id", as_index=False).agg({"pct_on_time": "mean"})
        route_order = df_ontime["route_id"].tolist()

    df_ontime["pct_on_time"] = pd.to_numeric(df_ontime["pct_on_time"], errors="coerce").fillna(0.0)
    df_ontime["pct_on_time"] = df_ontime["pct_on_time"].clip(0, 100)
    df_ontime["route_id"] = pd.Categorical(df_ontime["route_id"], categories=route_order, ordered=True)

    fig = px.bar(
        df_ontime,
        x="route_id",
        y="pct_on_time",
        labels={"route_id": "Ligne", "pct_on_time": "Ponctualité (%)"},
        title="Taux de ponctualité par ligne"
    )

    fig.update_xaxes(type="category", tickangle=-45)
    fig.update_yaxes(range=[0, 100], title_text="Ponctualité (%)")

    # largeur dynamique selon le nb de routes
    fig_width = max(900, len(df_ontime) * 30)  # 30px par barre
    fig.update_layout(
        width=fig_width,
        height=500,
        margin=dict(l=40, r=20, t=60, b=160),
        bargap=0.2
    )

    st.plotly_chart(fig, use_container_width=False)  # disable container_width pour activer le scroll

# --- KPI 5. Heatmap heures × jours ---
st.subheader("⏰ Retard moyen par heure et par jour de la semaine")

df_heatmap = load_parquet("kpi_delay_by_hour.parquet")

if not df_heatmap.empty:
    # Mapper les jours de la semaine
    dow_labels = ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"]
    df_heatmap["dow_name"] = df_heatmap["dow"].astype(int).map(lambda x: dow_labels[x])

    # Construire une matrice (jours x heures)
    df_pivot = df_heatmap.pivot(index="dow_name", columns="local_hour", values="avg_delay")

    # Tracer la heatmap
    fig = px.imshow(
        df_pivot,
        labels=dict(x="Heure", y="Jour", color="Retard moyen (min)"),
        x=df_pivot.columns,
        y=df_pivot.index,
        color_continuous_scale="Reds",
        aspect="auto"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Fichier kpi_delay_heatmap.parquet introuvable.")

# --- KPI 6. Taux de ponctualité global ---
st.subheader("🎯 Taux de ponctualité global")

df_global = load_parquet("kpi_global_ontime.parquet")
if df_global.empty:
    st.warning("Fichier kpi_global_ontime.parquet introuvable.")
else:
    pct_on_time = df_global["pct_on_time"].iloc[0]
    n_events = int(df_global["n_events"].iloc[0])

    # Créer un gauge Plotly
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=pct_on_time,
        number={'suffix': "%"},
        title={'text': "Ponctualité (≤5 min)"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "green"},
            'steps': [
                {'range': [0, 70], 'color': "red"},
                {'range': [70, 90], 'color': "orange"},
                {'range': [90, 100], 'color': "lightgreen"}
            ]
        }
    ))

    fig.update_layout(height=400, margin=dict(l=40, r=40, t=80, b=40))

    st.plotly_chart(fig, use_container_width=True)

    # Ajouter un petit résumé
    st.caption(f"Nombre total d'événements considérés : {n_events:,}")

# --- KPI 7. Evolution du retard par arrêt ---
st.subheader("📈 Évolution du retard par arrêt")

df_delay_stop = load_parquet("kpi_delay_stop.parquet")

if df_delay_stop.empty:
    st.warning("Fichier kpi_delay_stop.parquet introuvable.")
else:
    # Choisir entre stop_name (si dispo) ou stop_id
    df_delay_stop["stop_label"] = df_delay_stop["stop_name"].fillna(df_delay_stop["stop_id"])

    # Sélecteur multi-arrêts
    stops_available = df_delay_stop["stop_label"].unique().tolist()
    selected_stops = st.multiselect(
        "Sélectionnez un ou plusieurs arrêts à afficher",
        options=stops_available,
        default=stops_available[:3]
    )

    if selected_stops:
        df_filtered = df_delay_stop[df_delay_stop["stop_label"].isin(selected_stops)]

        fig = px.line(
            df_filtered,
            x="event_ts",
            y="avg_delay",
            color="stop_label",
            labels={"event_ts": "Heure", "avg_delay": "Retard moyen (min)", "stop_label": "Arrêt"},
            title="Évolution du retard par arrêt"
        )

        fig.update_layout(
            xaxis_title="Temps",
            yaxis_title="Retard moyen (minutes)",
            margin=dict(l=40, r=20, t=60, b=40),
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sélectionnez au moins un arrêt pour afficher le graphique.")

