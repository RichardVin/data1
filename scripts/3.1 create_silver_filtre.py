from pathlib import Path
import pandas as pd

# Chemins
base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "bronze" / "valeursfoncieres-2025" / "ValeursFoncieres-2025.parquet"
output_file = base_dir / "data" / "silver" / "valeursfoncieres_silver_filtre.parquet"

# Lecture
df = pd.read_parquet(input_file)

# Nettoyage des colonnes
df.columns = df.columns.str.strip()

# Colonnes à garder
columns_to_keep = [
    "No disposition",
    "Date mutation",
    "Nature mutation",
    "Valeur fonciere",
    "No voie",
    "B/T/Q",
    "Type de voie",
    "Code voie",
    "Voie",
    "Code postal",
    "Commune",
    "Code departement",
    "Code commune",
    "Prefixe de section",
    "Section",
    "No plan",
    "No Volume",
    "1er lot",
    "Surface Carrez du 1er lot",
    "2eme lot",
    "Surface Carrez du 2eme lot",
    "3eme lot",
    "Surface Carrez du 3eme lot",
    "4eme lot",
    "Surface Carrez du 4eme lot",
    "5eme lot",
    "Surface Carrez du 5eme lot",
    "Nombre de lots",
    "Code type local",
    "Type local",
    "Surface reelle bati",
    "Nombre pieces principales",
    "Nature culture",
    "Nature culture speciale",
    "Surface terrain"
]

# Sélection
df_silver = df[columns_to_keep].copy()

# -----------------------------
# FILTRES DE BASE
# -----------------------------
df_silver = df_silver[
    (df_silver["Valeur fonciere"].notna()) &
    (df_silver["Surface reelle bati"].notna()) &
    (df_silver["Valeur fonciere"] > 0) &
    (df_silver["Surface reelle bati"] > 0)
]

# Garde seulement le résidentiel classique
df_silver = df_silver[df_silver["Type local"].isin(["Maison", "Appartement"])].copy()

# Création du prix au m²
df_silver["prix_m2"] = df_silver["Valeur fonciere"] / df_silver["Surface reelle bati"]

# -----------------------------
# SUPPRESSION DES DOUBLONS
# -----------------------------

print("\nAvant suppression des doublons exacts :", len(df_silver))
df_silver = df_silver.drop_duplicates()
print("Après suppression des doublons exacts :", len(df_silver))

dedup_cols = [
    "Date mutation",
    "Valeur fonciere",
    "Code postal",
    "Commune",
    "Voie",
    "Type local",
    "Code voie"
]

print("\nAvant déduplication métier :", len(df_silver))

df_silver = df_silver.sort_values(
    by=["Date mutation", "Valeur fonciere", "Surface reelle bati"],
    ascending=[True, True, False]
)

df_silver = df_silver.drop_duplicates(
    subset=dedup_cols,
    keep="first"
)

print("Après déduplication métier :", len(df_silver))

# -----------------------------
# FLAGS QUALITÉ
# -----------------------------

# 1. Valeur foncière trop élevée
df_silver["flag_valeur_fonciere"] = (
    df_silver["Valeur fonciere"] <= 2_000_000
).astype(int)

# 2. Surface bâtie trop grande
df_silver["flag_surface_bati"] = (
    df_silver["Surface reelle bati"] <= 300
).astype(int)

# 3. Prix au m² cohérent
df_silver["flag_prix_m2"] = (
    (df_silver["prix_m2"] >= 500) &
    (df_silver["prix_m2"] <= 20_000)
).astype(int)

# 4. Problème potentiel sur les lots
# Ici on considère comme suspect :
# - plus d'un lot
# - ET aucune surface Carrez renseignée
surface_carrez_cols = [
    "Surface Carrez du 1er lot",
    "Surface Carrez du 2eme lot",
    "Surface Carrez du 3eme lot",
    "Surface Carrez du 4eme lot",
    "Surface Carrez du 5eme lot"
]

df_silver["has_surface_carrez"] = df_silver[surface_carrez_cols].notna().any(axis=1)

df_silver["flag_lot_problem"] = (
    (df_silver["Nombre de lots"].fillna(0) > 1) &
    (~df_silver["has_surface_carrez"])
).astype(int)

# 5. Flag global
df_silver["flag_ok"] = (
    (df_silver["flag_valeur_fonciere"] == 1) &
    (df_silver["flag_surface_bati"] == 1) &
    (df_silver["flag_prix_m2"] == 1) &
    (df_silver["flag_lot_problem"] == 0)
).astype(int)

# -----------------------------
# DATASET FINAL FILTRÉ
# -----------------------------
df_silver_filtre = df_silver[df_silver["flag_ok"] == 1].copy()

# Check
print("Nombre de lignes silver avant filtrage final :", len(df_silver))
print("Nombre de lignes silver après filtrage final :", len(df_silver_filtre))

print("\nRépartition des flags :")
print("flag_valeur_fonciere")
print(df_silver["flag_valeur_fonciere"].value_counts(dropna=False))
print("\nflag_surface_bati")
print(df_silver["flag_surface_bati"].value_counts(dropna=False))
print("\nflag_prix_m2")
print(df_silver["flag_prix_m2"].value_counts(dropna=False))
print("\nflag_lot_problem")
print(df_silver["flag_lot_problem"].value_counts(dropna=False))
print("\nflag_ok")
print(df_silver["flag_ok"].value_counts(dropna=False))

# Sauvegarde
df_silver_filtre.to_parquet(output_file, index=False)
print("✅ Silver filtré créé avec succès :", output_file)