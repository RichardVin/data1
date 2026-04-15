import pandas as pd

from pathlib import Path
import pandas as pd

# Dossier racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent

# Fichiers
input_file = BASE_DIR / "data" / "bronze" / "valeursfoncieres-2025/ValeursFoncieres-2025.txt"
output_file = BASE_DIR / "data" / "bronze" / "valeursfoncieres-2025/ValeursFoncieres-2025.parquet"

print("Lecture du fichier :", input_file)

df = pd.read_csv(
    input_file,
    sep="|",
    decimal=",",
    encoding="utf-8",
    low_memory=False
)

print(df.head())
print(df.dtypes)

df.to_parquet(output_file, index=False)

print(f"Fichier parquet créé : {output_file}")