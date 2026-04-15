from pathlib import Path
import pandas as pd

base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "silver" / "valeursfoncieres_silver.parquet"
input_silver_filtre = base_dir / "data" / "silver" / "valeursfoncieres_silver_filtre.parquet"

df = pd.read_parquet(input_silver_filtre)

print(df.head())
print(df.info())
print(df.columns.tolist())
print(df.describe(include="all"))

# Sample des 1000 premieres lignes
df_light =  df.head(1000)
#df.head(1000).to_csv("silver_sample 1000.csv", sep=";",  decimal=",", index=False)

#Sample aléatoire des 10 000 lignes
#df_sample = df.sample(n=10000, random_state=42)
df_sample = df.sample(frac=0.1, random_state=42)

#df_sample.to_csv("silver_filtre_sample 10%.csv", sep=";",  decimal=",", index=False)
#df.to_csv("sylver_all.csv", sep=";", index=False)

#Oberserver les données atypique de Sylver

