from pathlib import Path
import pandas as pd

base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "bronze" / "valeursfoncieres-2025/ValeursFoncieres-2025.parquet"

df = pd.read_parquet(input_file)

print(df.head())
print(df.info())
print(df.columns.tolist())
print(df.describe(include="all"))

df_light =  df.head(1000)
df.head(1000).to_csv("sample_bronze.csv", sep=";", index=False)