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