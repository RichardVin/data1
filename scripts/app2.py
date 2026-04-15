from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

# -----------------------------
# CONFIG PAGE
# -----------------------------
st.set_page_config(
    page_title="Dashboard Immobilier DVF",
    layout="wide"
)

st.title("Dashboard Immobilier DVF")
st.caption("Tables Gold générées à partir du Silver filtré")

# -----------------------------
# FORMATAGE
# -----------------------------
def format_int_fr(value):
    if pd.isna(value):
        return "0"
    return f"{int(round(value)):,}".replace(",", " ")

# -----------------------------
# CHEMINS
# -----------------------------
base_dir = Path(__file__).resolve().parent.parent
gold_dir = base_dir / "data" / "gold"

gold_transactions_file = gold_dir / "gold_transactions.parquet"

# -----------------------------
# CHARGEMENT
# -----------------------------
@st.cache_data
def load_data():
    df_transactions = pd.read_parquet(gold_transactions_file)
    return df_transactions

df_transactions = load_data()

# -----------------------------
# FILTRES SIDEBAR
# -----------------------------
st.sidebar.header("Filtres")

liste_departements = sorted(
    df_transactions["Code departement"].dropna().astype(str).unique().tolist()
)
selected_departements = st.sidebar.multiselect(
    "Département",
    options=liste_departements,
    default=[]
)

df_filtered = df_transactions.copy()

if selected_departements:
    df_filtered = df_filtered[
        df_filtered["Code departement"].astype(str).isin(selected_departements)
    ]

liste_communes = sorted(
    df_filtered["Commune"].dropna().astype(str).unique().tolist()
)
selected_communes = st.sidebar.multiselect(
    "Commune",
    options=liste_communes,
    default=[]
)

if selected_communes:
    df_filtered = df_filtered[
        df_filtered["Commune"].astype(str).isin(selected_communes)
    ]

liste_types = sorted(
    df_filtered["Type local"].dropna().astype(str).unique().tolist()
)
selected_types = st.sidebar.multiselect(
    "Type local",
    options=liste_types,
    default=[]
)

if selected_types:
    df_filtered = df_filtered[
        df_filtered["Type local"].astype(str).isin(selected_types)
    ]

# -----------------------------
# KPI
# -----------------------------
st.subheader("Indicateurs clés")

nb_transactions = len(df_filtered)
prix_m2_median = df_filtered["prix_m2"].median() if nb_transactions > 0 else 0
valeur_fonciere_mediane = df_filtered["Valeur fonciere"].median() if nb_transactions > 0 else 0
surface_mediane = df_filtered["Surface reelle bati"].median() if nb_transactions > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Nb transactions", format_int_fr(nb_transactions))
col2.metric("Prix médian au m²", f"{format_int_fr(prix_m2_median)} €")
col3.metric("Valeur foncière médiane", f"{format_int_fr(valeur_fonciere_mediane)} €")
col4.metric("Surface bâtie médiane", f"{format_int_fr(surface_mediane)} m²")

# -----------------------------
# TABLES AGREGEES DYNAMIQUES
# -----------------------------
st.subheader("Analyses")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Départements",
    "Communes",
    "Temps",
    "Distribution",
    "Détail"
])

with tab1:
    df_dep = (
        df_filtered
        .groupby("Code departement", dropna=False)
        .agg(
            nb_transactions=("prix_m2", "count"),
            prix_m2_median=("prix_m2", "median"),
            valeur_fonciere_mediane=("Valeur fonciere", "median"),
            surface_batie_mediane=("Surface reelle bati", "median")
        )
        .reset_index()
        .sort_values("nb_transactions", ascending=False)
    )

    for col in ["nb_transactions", "prix_m2_median", "valeur_fonciere_mediane", "surface_batie_mediane"]:
        df_dep[col] = df_dep[col].round(0).astype("Int64")

    st.dataframe(df_dep, use_container_width=True)

    if not df_dep.empty:
        fig_dep = px.bar(
            df_dep.sort_values("prix_m2_median", ascending=False),
            x="Code departement",
            y="prix_m2_median",
            text="prix_m2_median",
            title="Prix médian au m² par département"
        )
        fig_dep.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside"
        )
        fig_dep.update_layout(
            yaxis_title="Prix médian au m²",
            xaxis_title="Département"
        )
        st.plotly_chart(fig_dep, use_container_width=True)

with tab2:
    df_com = (
        df_filtered
        .groupby(["Code departement", "Commune"], dropna=False)
        .agg(
            nb_transactions=("prix_m2", "count"),
            prix_m2_median=("prix_m2", "median"),
            valeur_fonciere_mediane=("Valeur fonciere", "median")
        )
        .reset_index()
        .sort_values("nb_transactions", ascending=False)
    )

    df_com = df_com[df_com["nb_transactions"] >= 5].copy()

    for col in ["nb_transactions", "prix_m2_median", "valeur_fonciere_mediane"]:
        df_com[col] = df_com[col].round(0).astype("Int64")

    st.dataframe(df_com, use_container_width=True)

    if not df_com.empty:
        top_com = df_com.sort_values("prix_m2_median", ascending=False).head(20).copy()
        top_com["label"] = top_com["Commune"].astype(str) + " (" + top_com["Code departement"].astype(str) + ")"

        fig_com = px.bar(
            top_com,
            x="label",
            y="prix_m2_median",
            text="prix_m2_median",
            title="Top 20 communes par prix médian au m²"
        )
        fig_com.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside"
        )
        fig_com.update_layout(
            yaxis_title="Prix médian au m²",
            xaxis_title="Commune",
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_com, use_container_width=True)

with tab3:
    df_time = df_filtered.copy()
    df_time["Date mutation"] = pd.to_datetime(
        df_time["Date mutation"],
        dayfirst=True,
        errors="coerce"
    )
    df_time["annee_mois"] = df_time["Date mutation"].dt.to_period("M").astype(str)

    df_time = (
        df_time
        .dropna(subset=["annee_mois"])
        .groupby("annee_mois", dropna=False)
        .agg(
            nb_transactions=("prix_m2", "count"),
            prix_m2_median=("prix_m2", "median"),
            valeur_fonciere_mediane=("Valeur fonciere", "median")
        )
        .reset_index()
        .sort_values("annee_mois")
    )

    for col in ["nb_transactions", "prix_m2_median", "valeur_fonciere_mediane"]:
        df_time[col] = df_time[col].round(0).astype("Int64")

    st.dataframe(df_time, use_container_width=True)

    if not df_time.empty:
        fig_time = px.line(
            df_time,
            x="annee_mois",
            y="prix_m2_median",
            markers=True,
            title="Évolution du prix médian au m²"
        )
        fig_time.update_layout(
            yaxis_title="Prix médian au m²",
            xaxis_title="Mois"
        )
        st.plotly_chart(fig_time, use_container_width=True)

with tab4:
    col_a, col_b = st.columns(2)

    with col_a:
        fig_box = px.box(
            df_filtered,
            x="Type local",
            y="prix_m2",
            points=False,
            title="Distribution du prix au m² par type local"
        )
        fig_box.update_layout(
            xaxis_title="Type local",
            yaxis_title="Prix au m²"
        )
        st.plotly_chart(fig_box, use_container_width=True)

    with col_b:
        fig_hist = px.histogram(
            df_filtered,
            x="prix_m2",
            nbins=50,
            title="Histogramme du prix au m²"
        )
        fig_hist.update_layout(
            xaxis_title="Prix au m²",
            yaxis_title="Nombre de transactions"
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    col_c, col_d = st.columns(2)

    with col_c:
        fig_violin = px.violin(
            df_filtered,
            x="Type local",
            y="Valeur fonciere",
            box=True,
            points=False,
            title="Distribution de la valeur foncière par type local"
        )
        fig_violin.update_layout(
            xaxis_title="Type local",
            yaxis_title="Valeur foncière"
        )
        st.plotly_chart(fig_violin, use_container_width=True)

    with col_d:
        df_scatter = df_filtered.copy()
        if len(df_scatter) > 10000:
            df_scatter = df_scatter.sample(10000, random_state=42)

        fig_scatter = px.scatter(
            df_scatter,
            x="Surface reelle bati",
            y="Valeur fonciere",
            color="Type local",
            hover_data=["Commune", "Code departement", "prix_m2"],
            title="Surface bâtie vs valeur foncière"
        )
        fig_scatter.update_layout(
            xaxis_title="Surface bâtie",
            yaxis_title="Valeur foncière"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

with tab5:
    cols_detail = [
        "Date mutation",
        "Commune",
        "Code departement",
        "Type local",
        "Valeur fonciere",
        "Surface reelle bati",
        "Nombre pieces principales",
        "prix_m2",
        "Voie",
        "Code postal"
    ]

    cols_detail_existing = [col for col in cols_detail if col in df_filtered.columns]

    df_detail = df_filtered[cols_detail_existing].copy()
    df_detail = df_detail.sort_values(by="prix_m2", ascending=False)

    for col in ["Valeur fonciere", "Surface reelle bati", "Nombre pieces principales", "prix_m2"]:
        if col in df_detail.columns:
            df_detail[col] = df_detail[col].apply(
                lambda x: format_int_fr(x) if pd.notna(x) else ""
            )

    st.dataframe(df_detail, use_container_width=True)