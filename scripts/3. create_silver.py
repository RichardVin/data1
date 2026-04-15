from pathlib import Path
import pandas as pd

# Chemins
base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "bronze" / "valeursfoncieres-2025/ValeursFoncieres-2025.parquet"
output_file = base_dir / "data" / "silver" / "valeursfoncieres_silver.parquet"

# Lecture
df = pd.read_parquet(input_file)

# Nettoyage des colonnes (important même si pas de problème)
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
df_silver = df[columns_to_keep]

# Clean ---------------------------------------------------------------------------------
# --- FILTRAGE QUALITÉ ---

df_silver = df_silver[
    (df_silver["Valeur fonciere"].notna()) &
    (df_silver["Surface reelle bati"].notna()) &
    (df_silver["Valeur fonciere"] > 0) &
    (df_silver["Surface reelle bati"] > 0)
]

# --- FILTRAGE MÉTIER (recommandé) ---

df_silver = df_silver[df_silver["Type local"].isin(["Maison", "Appartement"])]

# --- CRÉATION PRIX AU M² ---

df_silver["prix_m2"] = df_silver["Valeur fonciere"] / df_silver["Surface reelle bati"]

# --- CHECK ---

print("Nombre de lignes après filtre :", len(df_silver))

# Sauvegarde--------------------------------------------------------------------------------
df_silver.to_parquet(output_file, index=False)

print("✅ Silver créé avec succès :", output_file)