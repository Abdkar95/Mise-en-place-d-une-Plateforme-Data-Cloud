# =============================================================================
# INGESTION BRONZE — S5, S6, S7
# Auteur : Abdoul-Karym
# Date   : 2026
#
# Ce script charge 3 fichiers sources dans BigQuery (couche Bronze/Raw).
#
# Fichiers traités :
#   S5 → S5_sinistres_as400_fixedwidth.txt  (Fixed-width)  → raw.raw_sinistres_as400
#   S6 → S6_marche_bloomberg_ticks.csv      (CSV)          → raw.raw_marche_bloomberg
#   S7 → S7_agences_commerciales.xlsx       (Excel)        → raw.raw_agences_commerciales
#
# Authentification GCP :
#   - Sur Google Colab      : automatique, rien à faire
#   - Sur Vertex AI / Cloud Shell : automatique, rien à faire
#   - En local sur ton PC   : lancer une fois dans le terminal :
#                             gcloud auth application-default login
# =============================================================================


# ==============================================================================
# ÉTAPE 0 — IMPORTS
# ==============================================================================

import pandas as pd                 # pour lire et manipuler les fichiers de données
from google.cloud import bigquery   # pour se connecter et envoyer des données à BigQuery
import warnings
warnings.filterwarnings("ignore")   # on masque les avertissements non critiques


# ==============================================================================
# ÉTAPE 1 — CONFIGURATION
# ==============================================================================

# ⚠️ Remplace la valeur ci-dessous par ton vrai ID de projet GCP
PROJECT_ID  = "TON_PROJECT_ID"   # exemple : "randstad-data-platform"
DATASET_RAW = "raw"              # dataset BigQuery qui reçoit les données brutes

# Chemins vers les fichiers sources
# On remonte d'un niveau (../) car ce script est dans src/
# et les fichiers sont dans data/external/
PATH_S5 = "../data/external/S5_sinistres_as400_fixedwidth.txt"
PATH_S6 = "../data/external/S6_marche_bloomberg_ticks.csv"
PATH_S7 = "../data/external/S7_agences_commerciales.xlsx"

# Connexion à BigQuery
# GCP détecte automatiquement tes credentials selon l'environnement utilisé
client = bigquery.Client(project=PROJECT_ID)

print("✅ Connexion à BigQuery réussie !")
print(f"   Projet  : {PROJECT_ID}")
print(f"   Dataset : {DATASET_RAW}")


# ==============================================================================
# ÉTAPE 2 — FONCTION DE CHARGEMENT RÉUTILISABLE
# ==============================================================================

def charger_dans_bigquery(df, nom_table):
    """
    Envoie un DataFrame pandas vers une table BigQuery.

    Arguments :
        df        : le DataFrame à envoyer
        nom_table : nom de la table dans BigQuery (ex: 'raw_sinistres_as400')
    """

    # L'adresse complète d'une table BigQuery = projet.dataset.table
    table_id = f"{PROJECT_ID}.{DATASET_RAW}.{nom_table}"

    # Paramètres du chargement
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE : si la table existe déjà, on la remplace entièrement
        # Pratique en développement pour recharger proprement à chaque test
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # autodetect : BigQuery devine automatiquement le type de chaque colonne
        autodetect=True
    )

    # On lance le chargement et on attend qu'il soit terminé avec .result()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"   ✅ Chargé dans BigQuery → {table_id} ({len(df)} lignes)")


# ==============================================================================
# ÉTAPE 3 — S5 : SINISTRES AS400 (FIXED-WIDTH)
# ==============================================================================
#
# C'est quoi un fichier fixed-width ?
# Un ancien format où chaque colonne occupe toujours le même nombre
# de caractères, sans virgule ni point-virgule comme séparateur.
#
# Exemple de ligne brute :
# SIN0000001 2024-01-15 POL001     INCENDIE   15000.00  OUVERT   AG001
#
# ⚠️ Les positions (colspecs) sont à ajuster selon la vraie structure du fichier.
#    Ouvre le fichier dans VS Code ou Notepad++ et compte les caractères.

print("\n📂 Lecture de S5 — Sinistres AS400...")

# Position de chaque colonne : (caractère_debut, caractère_fin)
colspecs_s5 = [
    (0,  10),   # id_sinistre
    (10, 20),   # date_sinistre
    (20, 35),   # code_police
    (35, 50),   # type_sinistre
    (50, 65),   # montant_sinistre
    (65, 75),   # statut
    (75, 85),   # code_agence
]

noms_colonnes_s5 = [
    "id_sinistre",
    "date_sinistre",
    "code_police",
    "type_sinistre",
    "montant_sinistre",
    "statut",
    "code_agence"
]

df_s5 = pd.read_fwf(
    PATH_S5,
    colspecs=colspecs_s5,
    names=noms_colonnes_s5,
    encoding="latin-1",  # encodage fréquent sur les systèmes AS400
    skiprows=1           # ignore la première ligne si c'est un en-tête
)

# -- Nettoyage --
# On supprime les lignes entièrement vides
df_s5 = df_s5.dropna(how="all")

# On supprime les espaces avant/après dans les colonnes texte
# (très fréquent dans les fichiers fixed-width)
for col in df_s5.select_dtypes(include="object").columns:
    df_s5[col] = df_s5[col].str.strip()

# -- Contrôle qualité --
print(f"   Lignes lues       : {df_s5.shape[0]}")
print(f"   Colonnes          : {df_s5.shape[1]}")
print(f"   Valeurs nulles    : {df_s5.isnull().sum().sum()}")
print(f"   Doublons          : {df_s5.duplicated().sum()}")

# -- Chargement BigQuery --
print("   ⏳ Envoi vers BigQuery...")
charger_dans_bigquery(df_s5, "raw_sinistres_as400")


# ==============================================================================
# ÉTAPE 4 — S6 : DONNÉES DE MARCHÉ BLOOMBERG (CSV)
# ==============================================================================
#
# C'est quoi un CSV ?
# Un fichier texte où les colonnes sont séparées par des virgules.
# C'est le format de données le plus courant, lu directement avec pandas.
#
# 💡 Note pour plus tard : les données Bloomberg sont souvent volumineuses.
#    En semaine 2, on ajoutera un partitionnement par date dans BigQuery
#    pour optimiser les coûts et les performances.

print("\n📂 Lecture de S6 — Marché Bloomberg...")

df_s6 = pd.read_csv(
    PATH_S6,
    sep=",",          # séparateur de colonnes (remplace par ";" si nécessaire)
    encoding="utf-8"  # encodage du fichier
)

# -- Nettoyage --
# On normalise les noms de colonnes : minuscules + underscores à la place des espaces
# Exemple : "Prix Marché" devient "prix_marche"
df_s6.columns = [
    col.strip().lower().replace(" ", "_").replace("-", "_")
    for col in df_s6.columns
]

# On supprime les lignes vides et les doublons exacts
df_s6 = df_s6.dropna(how="all")
df_s6 = df_s6.drop_duplicates()

# -- Contrôle qualité --
print(f"   Lignes lues       : {df_s6.shape[0]}")
print(f"   Colonnes          : {df_s6.shape[1]}")
print(f"   Valeurs nulles    : {df_s6.isnull().sum().sum()}")
print(f"   Doublons          : {df_s6.duplicated().sum()}")

# -- Chargement BigQuery --
print("   ⏳ Envoi vers BigQuery...")
charger_dans_bigquery(df_s6, "raw_marche_bloomberg")


# ==============================================================================
# ÉTAPE 5 — S7 : AGENCES COMMERCIALES (EXCEL)
# ==============================================================================
#
# C'est quoi ce fichier ici ?
# C'est un référentiel : il liste les agences avec leurs codes et informations.
# Dans notre schéma en étoile, il deviendra la table de dimension dim_agences.

print("\n📂 Lecture de S7 — Agences Commerciales...")

df_s7 = pd.read_excel(
    PATH_S7,
    engine="openpyxl",  # bibliothèque nécessaire pour lire les fichiers .xlsx
    sheet_name=0        # on lit le premier onglet
                        # si l'onglet a un nom précis : sheet_name="Agences"
)

# -- Nettoyage --
# Normalisation des noms de colonnes
df_s7.columns = [
    col.strip().lower().replace(" ", "_").replace("-", "_")
    for col in df_s7.columns
]

# Les fichiers Excel contiennent souvent des lignes vides entre les données
df_s7 = df_s7.dropna(how="all")
df_s7 = df_s7.drop_duplicates()

# -- Contrôle qualité --
print(f"   Lignes lues       : {df_s7.shape[0]}")
print(f"   Colonnes          : {df_s7.shape[1]}")
print(f"   Valeurs nulles    : {df_s7.isnull().sum().sum()}")
print(f"   Doublons          : {df_s7.duplicated().sum()}")

# -- Chargement BigQuery --
print("   ⏳ Envoi vers BigQuery...")
charger_dans_bigquery(df_s7, "raw_agences_commerciales")


# ==============================================================================
# ÉTAPE 6 — RÉCAPITULATIF FINAL
# ==============================================================================

print("\n" + "=" * 60)
print("🎉 INGESTION BRONZE TERMINÉE")
print("=" * 60)

recap = {
    "S5 → raw_sinistres_as400"      : len(df_s5),
    "S6 → raw_marche_bloomberg"     : len(df_s6),
    "S7 → raw_agences_commerciales" : len(df_s7),
}

for table, nb_lignes in recap.items():
    print(f"   ✅ {table} : {nb_lignes} lignes")

# -- Vérification directe dans BigQuery --
# On envoie une requête SQL pour confirmer que les données sont bien arrivées
print("\n🔍 Vérification dans BigQuery :")

tables = [
    "raw_sinistres_as400",
    "raw_marche_bloomberg",
    "raw_agences_commerciales"
]

for table in tables:
    query    = f"SELECT COUNT(*) as nb_lignes FROM `{PROJECT_ID}.{DATASET_RAW}.{table}`"
    resultat = client.query(query).result()
    for ligne in resultat:
        print(f"   ✅ {table} → {ligne.nb_lignes} lignes confirmées dans BigQuery")
