from pathlib import Path
import duckdb
import os

# -----------------------------
# CHEMINS
# -----------------------------
base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "silver" / "valeursfoncieres_silver_filtre.parquet"
gold_dir = base_dir / "data" / "gold"

gold_dir.mkdir(parents=True, exist_ok=True)

gold_transactions_file = gold_dir / "gold_transactions.parquet"
gold_departement_file = gold_dir / "gold_departement.parquet"
gold_commune_file = gold_dir / "gold_commune.parquet"
gold_mensuel_file = gold_dir / "gold_mensuel.parquet"
gold_type_local_file = gold_dir / "gold_type_local.parquet"

# -----------------------------
# CONNEXION DUCKDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# TABLE DETAIL (drill down)
# -----------------------------
con.execute(f"""
COPY (
    SELECT *
    FROM read_parquet('{input_file.as_posix()}')
) TO '{gold_transactions_file.as_posix()}' (FORMAT PARQUET);
""")

# -----------------------------
# TABLE GOLD PAR DEPARTEMENT
# -----------------------------
con.execute(f"""
COPY (
    SELECT
        "Code departement" AS code_departement,
        COUNT(*) AS nb_transactions,
        ROUND(AVG(prix_m2), 2) AS prix_m2_moyen,
        ROUND(MEDIAN(prix_m2), 2) AS prix_m2_median,
        ROUND(AVG("Valeur fonciere"), 2) AS valeur_fonciere_moyenne,
        ROUND(AVG("Surface reelle bati"), 2) AS surface_batie_moyenne
    FROM read_parquet('{input_file.as_posix()}')
    GROUP BY "Code departement"
    ORDER BY nb_transactions DESC
) TO '{gold_departement_file.as_posix()}' (FORMAT PARQUET);
""")

# -----------------------------
# TABLE GOLD PAR COMMUNE
# -----------------------------
con.execute(f"""
COPY (
    SELECT
        "Code departement" AS code_departement,
        "Commune" AS commune,
        COUNT(*) AS nb_transactions,
        ROUND(AVG(prix_m2), 2) AS prix_m2_moyen,
        ROUND(MEDIAN(prix_m2), 2) AS prix_m2_median,
        ROUND(AVG("Valeur fonciere"), 2) AS valeur_fonciere_moyenne,
        ROUND(AVG("Surface reelle bati"), 2) AS surface_batie_moyenne
    FROM read_parquet('{input_file.as_posix()}')
    GROUP BY "Code departement", "Commune"
    ORDER BY nb_transactions DESC
) TO '{gold_commune_file.as_posix()}' (FORMAT PARQUET);
""")

# -----------------------------
# TABLE GOLD PAR MOIS
# -----------------------------
con.execute(f"""
COPY (
    SELECT
        strftime(TRY_CAST("Date mutation" AS DATE), '%Y-%m') AS annee_mois,
        COUNT(*) AS nb_transactions,
        ROUND(AVG(prix_m2), 2) AS prix_m2_moyen,
        ROUND(MEDIAN(prix_m2), 2) AS prix_m2_median,
        ROUND(AVG("Valeur fonciere"), 2) AS valeur_fonciere_moyenne
    FROM read_parquet('{input_file.as_posix()}')
    WHERE TRY_CAST("Date mutation" AS DATE) IS NOT NULL
    GROUP BY annee_mois
    ORDER BY annee_mois
) TO '{gold_mensuel_file.as_posix()}' (FORMAT PARQUET);
""")

# -----------------------------
# TABLE GOLD PAR TYPE DE BIEN
# -----------------------------
con.execute(f"""
COPY (
    SELECT
        "Type local" AS type_local,
        COUNT(*) AS nb_transactions,
        ROUND(AVG(prix_m2), 2) AS prix_m2_moyen,
        ROUND(MEDIAN(prix_m2), 2) AS prix_m2_median,
        ROUND(AVG("Valeur fonciere"), 2) AS valeur_fonciere_moyenne,
        ROUND(AVG("Surface reelle bati"), 2) AS surface_batie_moyenne
    FROM read_parquet('{input_file.as_posix()}')
    GROUP BY "Type local"
    ORDER BY nb_transactions DESC
) TO '{gold_type_local_file.as_posix()}' (FORMAT PARQUET);
""")

# -----------------------------
# CHECK
# -----------------------------
print("✅ Tables gold créées avec succès :")
print("-", gold_transactions_file)
print("-", gold_departement_file)
print("-", gold_commune_file)
print("-", gold_mensuel_file)
print("-", gold_type_local_file)

con.close()