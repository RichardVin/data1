from pathlib import Path
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

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
# FONCTION FORMAT
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
gold_departement_file = gold_dir / "gold_departement.parquet"
gold_commune_file = gold_dir / "gold_commune.parquet"
gold_mensuel_file = gold_dir / "gold_mensuel.parquet"
gold_type_local_file = gold_dir / "gold_type_local.parquet"

# -----------------------------
# CHARGEMENT DES DONNEES
# -----------------------------
@st.cache_data
def load_data():
    df_transactions = pd.read_parquet(gold_transactions_file)
    df_departement = pd.read_parquet(gold_departement_file)
    df_commune = pd.read_parquet(gold_commune_file)
    df_mensuel = pd.read_parquet(gold_mensuel_file)
    df_type_local = pd.read_parquet(gold_type_local_file)
    return df_transactions, df_departement, df_commune, df_mensuel, df_type_local


(
    df_transactions,
    df_departement,
    df_commune,
    df_mensuel,
    df_type_local
) = load_data()

# -----------------------------
# SIDEBAR FILTRES
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

df_transactions_filtered = df_transactions.copy()

if selected_departements:
    df_transactions_filtered = df_transactions_filtered[
        df_transactions_filtered["Code departement"].astype(str).isin(selected_departements)
    ]

liste_communes = sorted(
    df_transactions_filtered["Commune"].dropna().astype(str).unique().tolist()
)
selected_communes = st.sidebar.multiselect(
    "Commune",
    options=liste_communes,
    default=[]
)

if selected_communes:
    df_transactions_filtered = df_transactions_filtered[
        df_transactions_filtered["Commune"].astype(str).isin(selected_communes)
    ]

liste_types = sorted(
    df_transactions_filtered["Type local"].dropna().astype(str).unique().tolist()
)
selected_types = st.sidebar.multiselect(
    "Type local",
    options=liste_types,
    default=[]
)

if selected_types:
    df_transactions_filtered = df_transactions_filtered[
        df_transactions_filtered["Type local"].astype(str).isin(selected_types)
    ]

# -----------------------------
# KPI PRINCIPAUX
# -----------------------------
st.subheader("Indicateurs clés")

nb_transactions = len(df_transactions_filtered)
prix_m2_median = df_transactions_filtered["prix_m2"].median() if nb_transactions > 0 else 0
valeur_fonciere_median = df_transactions_filtered["Valeur fonciere"].median() if nb_transactions > 0 else 0
surface_mediane = df_transactions_filtered["Surface reelle bati"].median() if nb_transactions > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Nb transactions", format_int_fr(nb_transactions))
col2.metric("Prix médian au m²", f"{format_int_fr(prix_m2_median)} €")
col3.metric("Valeur foncière médiane", f"{format_int_fr(valeur_fonciere_median)} €")
col4.metric("Surface bâtie médiane", f"{format_int_fr(surface_mediane)} m²")

# -----------------------------
# BOXPLOTS
# -----------------------------
st.subheader("Distribution des prix")

col_box1, col_box2 = st.columns(2)

with col_box1:
    st.markdown("**Box plot du prix au m²**")
    fig1, ax1 = plt.subplots(figsize=(6, 4))
    ax1.boxplot(df_transactions_filtered["prix_m2"].dropna())
    ax1.set_ylabel("Prix au m²")
    st.pyplot(fig1)

with col_box2:
    st.markdown("**Box plot de la valeur foncière**")
    fig2, ax2 = plt.subplots(figsize=(6, 4))
    ax2.boxplot(df_transactions_filtered["Valeur fonciere"].dropna())
    ax2.set_ylabel("Valeur foncière")
    st.pyplot(fig2)

# -----------------------------
# AGREGATION DYNAMIQUE
# -----------------------------
st.subheader("Analyses")

tab1, tab2, tab3, tab4 = st.tabs([
    "Par département",
    "Par commune",
    "Par mois",
    "Par type local"
])

with tab1:
    df_dep_view = (
        df_transactions_filtered
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

    st.dataframe(df_dep_view, use_container_width=True)

    if not df_dep_view.empty:
        st.bar_chart(
            df_dep_view.set_index("Code departement")["prix_m2_median"]
        )

with tab2:
    df_commune_view = (
        df_transactions_filtered
        .groupby(["Code departement", "Commune"], dropna=False)
        .agg(
            nb_transactions=("prix_m2", "count"),
            prix_m2_median=("prix_m2", "median"),
            valeur_fonciere_mediane=("Valeur fonciere", "median")
        )
        .reset_index()
        .sort_values("nb_transactions", ascending=False)
    )

    st.dataframe(df_commune_view, use_container_width=True)

    if not df_commune_view.empty:
        top_communes = df_commune_view.head(20).copy()
        top_communes["label"] = top_communes["Commune"].astype(str) + " (" + top_communes["Code departement"].astype(str) + ")"
        st.bar_chart(
            top_communes.set_index("label")["prix_m2_median"]
        )

with tab3:
    df_mensuel_view = df_transactions_filtered.copy()
    df_mensuel_view["Date mutation"] = pd.to_datetime(
        df_mensuel_view["Date mutation"],
        dayfirst=True,
        errors="coerce"
    )
    df_mensuel_view["annee_mois"] = df_mensuel_view["Date mutation"].dt.to_period("M").astype(str)

    df_mensuel_view = (
        df_mensuel_view
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

    st.dataframe(df_mensuel_view, use_container_width=True)

    if not df_mensuel_view.empty:
        st.line_chart(
            df_mensuel_view.set_index("annee_mois")["prix_m2_median"]
        )

with tab4:
    df_type_view = (
        df_transactions_filtered
        .groupby("Type local", dropna=False)
        .agg(
            nb_transactions=("prix_m2", "count"),
            prix_m2_median=("prix_m2", "median"),
            valeur_fonciere_mediane=("Valeur fonciere", "median"),
            surface_batie_mediane=("Surface reelle bati", "median")
        )
        .reset_index()
        .sort_values("nb_transactions", ascending=False)
    )

    st.dataframe(df_type_view, use_container_width=True)

    if not df_type_view.empty:
        st.bar_chart(
            df_type_view.set_index("Type local")["prix_m2_median"]
        )

# -----------------------------
# DETAIL DES TRANSACTIONS
# -----------------------------
st.subheader("Détail des transactions")

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

cols_detail_existing = [col for col in cols_detail if col in df_transactions_filtered.columns]

df_detail = df_transactions_filtered[cols_detail_existing].copy()

for col in ["Valeur fonciere", "Surface reelle bati", "Nombre pieces principales", "prix_m2"]:
    if col in df_detail.columns:
        df_detail[col] = df_detail[col].apply(
            lambda x: format_int_fr(x) if pd.notna(x) else ""
        )

st.dataframe(
    df_detail.sort_values(by="prix_m2", ascending=False),
    use_container_width=True
)