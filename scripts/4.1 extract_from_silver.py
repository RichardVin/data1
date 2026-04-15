from pathlib import Path
import pandas as pd

# -----------------------------
# PARAMETRE A MODIFIER
# -----------------------------
CODE_POSTAL = "77186"   # ← mets ton code ici

# -----------------------------
# CHEMINS
# -----------------------------
base_dir = Path(__file__).resolve().parent.parent
input_file = base_dir / "data" / "silver" / "valeursfoncieres_silver_filtre.parquet"

# -----------------------------
# LECTURE
# -----------------------------
df = pd.read_parquet(input_file)

# -----------------------------
# NETTOYAGE CODE POSTAL
# -----------------------------
df["Code postal"] = (
    df["Code postal"]
    .astype(str)
    .str.replace(".0", "", regex=False)  # enlève les .0
    .str.strip()
)

# -----------------------------
# FILTRE
# -----------------------------
df_extract = df[df["Code postal"] == CODE_POSTAL].copy()

# -----------------------------
# RESULTAT
# -----------------------------
print(f"\nCode postal recherché : {CODE_POSTAL}")
print(f"Nombre de lignes trouvées : {len(df_extract)}")

if len(df_extract) > 0:
    print("\nAperçu :")
    print(df_extract.head(20))

    # Export CSV
    output_file = base_dir / "data" / "silver" / f"extract_cp_{CODE_POSTAL}.csv"
    df_extract.to_csv(output_file, sep=";", decimal=",", index=False, encoding="utf-8-sig")

    print(f"\n✅ Export créé : {output_file}")
else:
    print("\n❌ Aucun résultat trouvé")

    # DEBUG utile
    print("\nExemples de codes postaux dans le dataset :")
    print(df["Code postal"].dropna().unique()[:10])