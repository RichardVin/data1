from pathlib import Path
import pandas as pd

# -----------------------------
# CHEMINs
# -----------------------------
base_dir = Path(__file__).resolve().parent.parent
gold_dir = base_dir / "data" / "gold"

gold_transactions_file = gold_dir / "gold_transactions.parquet"
gold_departement_file = gold_dir / "gold_departement.parquet"
gold_commune_file = gold_dir / "gold_commune.parquet"
gold_mensuel_file = gold_dir / "gold_mensuel.parquet"
gold_type_local_file = gold_dir / "gold_type_local.parquet"

# -----------------------------
# CHARGEMENT
# -----------------------------
df_transactions = pd.read_parquet(gold_transactions_file)
df_departement = pd.read_parquet(gold_departement_file)
df_commune = pd.read_parquet(gold_commune_file)
df_mensuel = pd.read_parquet(gold_mensuel_file)
df_type_local = pd.read_parquet(gold_type_local_file)

# -----------------------------
# EXPLORATION SIMPLE
# -----------------------------

print("\n================ TRANSACTIONS ================")
print(df_transactions.head())
print(df_transactions.shape)

print("\n================ DEPARTEMENT ================")
print(df_departement.head())
print(df_departement.shape)
print(df_departement.describe())

print("\n================ COMMUNE ================")
print(df_commune.head())



print("\n================ MENSUEL ================")
print(df_mensuel.head())
print(df_mensuel.shape)

print("\n================ TYPE LOCAL ================")
print(df_type_local.head())
print(df_type_local.shape)

# -----------------------------
# ANALYSE RAPIDE (ULTRA UTILE)
# -----------------------------

print("\nTop 10 départements (prix m2 médian)")
print(df_departement.sort_values("prix_m2_median", ascending=False).head(10))

print("\nTop 10 communes (prix m2 médian)")
print(df_commune.sort_values("prix_m2_median", ascending=False).head(10))

print("\nEvolution mensuelle")
print(df_mensuel)

print("\nRépartition par type de bien")
print(df_type_local)