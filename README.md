# Plateforme Data Cloud – Randstad Digital

## Présentation du Projet

Ce projet consiste à concevoir et mettre en place une plateforme Data Cloud moderne pour Randstad Digital afin de centraliser, transformer, stocker et exploiter efficacement les données provenant de plusieurs sources métiers.

L’objectif principal est de construire une architecture Data scalable, sécurisée et performante basée sur Google Cloud Platform (GCP).

Le projet s’inscrit dans une démarche de transformation digitale orientée Data & Cloud.

---

# Contexte du Projet

Dans le cadre de l’évolution de ses besoins analytiques, Randstad Digital Lille souhaite renforcer son écosystème Data afin de :

- centraliser les données provenant de plusieurs systèmes,
- automatiser les flux de traitement,
- améliorer la qualité des données,
- fournir des datasets fiables aux équipes métiers,
- optimiser la création de dashboards décisionnels,
- améliorer les performances analytiques.

---

# Objectifs du Projet

## Objectif Principal

Mettre en place une plateforme Data Engineering performante basée sur Google Cloud Platform permettant :

- l’intégration des données,
- leur transformation,
- leur stockage,
- leur exploitation analytique.

---

# Objectifs Secondaires

## Centralisation des Données
Regrouper les données provenant de :
- ERP,
- CRM,
- APIs,
- fichiers CSV / Excel,
- bases de données relationnelles.

## Automatisation des Flux
- Développer des pipelines ETL / ELT
- Automatiser les traitements de données
- Gérer les logs et les erreurs

## Qualité & Gouvernance
- Contrôle qualité des données
- Gestion des doublons
- Validation des formats
- Traçabilité des traitements
- Gestion des accès

## Optimisation Analytique
- Création de Data Marts
- Préparation des datasets BI
- Optimisation des requêtes SQL

## Scalabilité Cloud
- Construction d’une architecture scalable sur GCP
- Optimisation des performances BigQuery
- Réduction des coûts Cloud

---

# Technologies Utilisées

## Cloud & Infrastructure
- Google Cloud Platform (GCP)
- BigQuery
- Cloud Storage
- Cloud Functions
- Cloud Composer
- Pub/Sub
- IAM

## ETL / ELT
- Talend
- Stambia

## Langages
- SQL
- Python

## Business Intelligence
- Power BI
- Tableau
- Looker

## DevOps / Collaboration
- Git
- GitHub
- Jira
- CI/CD

---

# Architecture Technique

```text
Sources de données
│
├── ERP
├── CRM
├── APIs
└── Fichiers CSV / Excel

        ↓

ETL / ELT
│
├── Talend
└── Stambia

        ↓

Stockage Cloud
│
├── Google Cloud Storage
└── BigQuery

        ↓

Transformation & Modélisation
│
├── Data Warehouse
└── Data Marts

        ↓

Restitution & Visualisation
│
├── Power BI
├── Tableau
└── Looker
```

---

# Structure du Repository

```text
data/
│
├── raw/         -> Données brutes
├── processed/   -> Données nettoyées et transformées
└── external/    -> Données externes complémentaires

sql/              -> Scripts SQL
src/              -> Scripts Python
notebooks/        -> Analyses exploratoires
dashboard/        -> Dashboards BI
docs/             -> Documentation technique
reports/          -> Rapports et présentations
```
---

dim_clients ──────┐
dim_agences ──────┤
dim_contrats ─────┼──── fact_transactions
dim_temps ────────┤
dim_produits ─────┘
---

# Répartition des Responsabilités

## Traoré Abdoul-Karym
### Chef de Projet & Coordination
- Gestion du projet
- Organisation Jira
- Planification Agile
- Gestion GitHub
- Documentation
- Coordination de l’équipe

---

## Jean Yves Kpangban
### Data Engineering & SQL
- Développement des pipelines ETL/ELT
- Nettoyage et transformation des données
- Requêtes SQL
- Modélisation des données
- Gestion BigQuery

---

## Boniface Péré
### Business Intelligence & Analyse
- Création des dashboards
- Création des KPIs
- Analyse des données
- Visualisation des données
- Support analytique

---

# Méthodologie de Travail

Le projet suit une méthodologie Agile Scrum.

## Organisation
- Sprint Planning
- Daily Meetings
- Revues de Sprint
- Rétrospectives

## Gestion des Tâches
Les tâches sont organisées et réparties via Jira.

Chaque membre travaille sur une partie spécifique du projet à travers des branches Git dédiées.

---

# Gestion des Branches Git

## Branches Principales

- `main` → version stable
- `dev` → branche d’intégration

## Branches de Développement

- `feature/project-management`
- `feature/data-engineering`
- `feature/dashboard-bi`

---

# Fonctionnalités du Projet

- Centralisation des données
- Pipelines ETL/ELT automatisés
- Data Warehouse sur BigQuery
- Création de Data Marts
- Dashboards décisionnels
- Optimisation SQL
- Gouvernance des données
- Monitoring des pipelines

---

# Résultats Attendus

- Automatisation des flux de données
- Meilleure qualité des données
- Réduction des temps de traitement
- Dashboards fiables et performants
- Architecture scalable et sécurisée
- Amélioration de la prise de décision

---

# Perspectives d’Évolution

- Intégration du Machine Learning
- Analyse prédictive
- Streaming temps réel
- Automatisation avancée
- Mise en place d’un Data Lake

---

# Auteurs

- Traoré Abdoul-Karym
- Jean Yves Kpangban
- Boniface Péré

---

# DONNEES SYNTHETIQUES - Secteur Bancaire/Assurance France
## Volume entreprise - Compatible GCP Free Tier

## LIMITES GCP FREE TIER RESPECTEES
- Cloud Storage  : 5 GB free  -> fichiers totaux < 1 GB compresses
- BigQuery       : 10 GB storage + 1 TB requetes/mois -> OK
- Cloud Composer : utiliser trial 90 jours GCP

## VOLUMES GENERES
| Fichier                          | Format      | Lignes/Entrees | Taille  |
|----------------------------------|-------------|----------------|---------|
| S1_transactions_core_banking.csv | CSV pipe    | ~154 500       | ~15 MB  |
| S2_clients_kyc.jsonl             | JSON Lines  | ~51 500        | ~10 MB  |
| S3_contrats_guidewire.xml        | XML         | 5 000          | ~4 MB   |
| S4_swift_mt103.txt               | Texte MT    | 30 000         | ~12 MB  |
| S5_sinistres_as400_fixedwidth.txt| Fixed-Width | 200 000        | ~22 MB  |
| S6_marche_bloomberg_ticks.csv    | CSV         | 500 000        | ~50 MB  |
| S7_agences_commerciales.xlsx     | Excel 3 ong.| 15 000         | ~4 MB   |
| S8_scoring_aml.jsonl             | JSON Lines  | ~82 400        | ~30 MB  |
| S9_reporting_acpr_corep.csv      | CSV sep-;   | 640            | <1 MB   |
| S10_logs_digitaux.jsonl          | JSON Lines  | ~515 000       | ~75 MB  |
| TOTAL                            |             | ~1 554 000     | ~222 MB |

## INGESTION GCP RECOMMANDEE

1. Uploader dans Cloud Storage (bucket : gs://randstad-data-raw/)
2. S1, S5, S6, S9 -> Cloud Data Fusion (CSV / Fixed-Width)
3. S2, S8, S10 -> Dataflow (JSONL streaming/batch)
4. S3 -> Dataproc Spark (XML distribué)
5. S4 -> Cloud Functions (parser SWIFT MT103)
6. S7 -> Cloud Functions (traitement Excel)
