# 🏠 DVF Data Pipeline & Dashboard

## 📌 Objectif

Construire un pipeline data complet à partir des données DVF (transactions immobilières en France) :

- ingestion de données brutes
- nettoyage et transformation (architecture médallion)
- création de KPI métier
- visualisation via dashboard interactif

---

## 🧱 Architecture

Bronze → Silver → Gold

- **Bronze** : données brutes DVF
- **Silver** : nettoyage + déduplication + création de variables (prix/m²)
- **Gold** : agrégations pour analyse

---

## ⚙️ Stack technique

- Python (Pandas)
- DuckDB / Parquet
- Streamlit (dashboard)
- Matplotlib / Plotly

---

## 🔄 Pipeline

1. Extraction des données DVF
2. Nettoyage des données
3. Déduplication des transactions
4. Création du prix au m²
5. Filtrage des données aberrantes
6. Création des tables d'analyse (Gold)
7. Visualisation via Streamlit

---

## 📊 KPI calculés

- Prix médian au m²
- Valeur foncière médiane
- Surface moyenne
- Nombre de transactions

---

## 🚀 Lancer le projet

```bash
pip install -r requirements.txt
streamlit run app/app.py


_______________________________________________________________________________________

# Nettoyage Silver (DVF)

1. Sélection des colonnes utiles

2. Filtrage de base :
   - Valeur foncière non nulle et > 0
   - Surface réelle bâtie non nulle et > 0

3. Filtrage métier :
   - Conservation uniquement des biens résidentiels :
     (Maison, Appartement)

4. Création du KPI :
   - prix_m2 = Valeur fonciere / Surface reelle bati

5. Suppression des doublons :
   - Suppression des doublons exacts
   - Déduplication métier sur :
     (Date, Valeur, Code postal, Commune, Voie, Type local, Code voie)
   - Conservation de la ligne la plus pertinente (plus grande surface)

6. Contrôles qualité (flags) :
   - Valeur foncière ≤ 2 000 000 €
   - Surface bâtie ≤ 300 m²
   - 500 € ≤ prix_m2 ≤ 20 000 €
   - Exclusion des lots multiples sans surface Carrez

7. Filtrage final :
   - Conservation uniquement des lignes avec flag_ok = 1

→ Résultat : dataset propre, cohérent et prêt pour analyse (Gold)