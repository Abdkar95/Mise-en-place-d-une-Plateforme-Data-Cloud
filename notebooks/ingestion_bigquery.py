"""
=============================================================
  INGESTION COMPLÈTE — Cloud Storage → BigQuery
  Projet : Plateforme Data Cloud Randstad Digital
  Auteurs : Traoré Abdoul-Karym, Jean Yves Kpangban, Boniface Péré
=============================================================

PRÉREQUIS :
  pip install google-cloud-bigquery google-cloud-storage pandas openpyxl xmltodict

AUTHENTIFICATION GCP :
  gcloud auth application-default login
  OU définir la variable :
  export GOOGLE_APPLICATION_CREDENTIALS="/chemin/vers/service-account.json"

UTILISATION :
  python ingestion_bigquery.py
"""

import io
import re
import json
import logging
import xmltodict
import pandas as pd
from google.cloud import storage, bigquery

# ─────────────────────────────────────────────
#  CONFIGURATION — À ADAPTER À VOTRE PROJET
# ─────────────────────────────────────────────
GCP_PROJECT_ID   = "randstad-data-platform"          # ex: "randstad-digital-prod"
GCS_BUCKET_NAME  = "db_randstad"         # nom de votre bucket Cloud Storage
BQ_DATASET_RAW   = "Bronze"               # dataset BigQuery cible (couche Raw)
GCS_PREFIX       = "external/"                 # sous-dossier dans le bucket (si applicable)

# ─────────────────────────────────────────────
#  INITIALISATION DES CLIENTS GCP
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

storage_client = storage.Client(project=GCP_PROJECT_ID)
bq_client      = bigquery.Client(project=GCP_PROJECT_ID)
bucket         = storage_client.bucket(GCS_BUCKET_NAME)


# ─────────────────────────────────────────────
#  UTILITAIRES COMMUNS
# ─────────────────────────────────────────────
def ensure_dataset(dataset_id: str):
    """Crée le dataset BigQuery s'il n'existe pas."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    log.info(f"Dataset prêt : {dataset_id}")


def load_df_to_bq(df: pd.DataFrame, table_name: str, write_mode="WRITE_TRUNCATE"):
    """
    Charge un DataFrame Pandas dans BigQuery.
    write_mode : WRITE_TRUNCATE (écrase) | WRITE_APPEND (ajoute)
    """
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,          # BigQuery détecte les types automatiquement
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Attendre la fin du job
    log.info(f"✅ {table_name} chargé — {len(df):,} lignes → {table_id}")


def read_gcs_file(blob_name: str) -> bytes:
    """Télécharge un fichier depuis Cloud Storage en mémoire."""
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    log.info(f"Fichier téléchargé : gs://{GCS_BUCKET_NAME}/{blob_name} ({len(data)/1024/1024:.1f} MB)")
    return data


# ─────────────────────────────────────────────
#  S1 — TRANSACTIONS CORE BANKING (CSV pipe)
# ─────────────────────────────────────────────
def ingest_s1_transactions():
    """
    Format  : CSV avec séparateur | (pipe)
    Méthode : Chargement natif BigQuery depuis Cloud Storage
    Pourquoi: CSV standard — BigQuery peut le charger directement sans Python
    """
    log.info("── S1 : Transactions Core Banking ──")

    # Option A : chargement natif BQ (recommandé pour les gros CSV)
    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.s1_transactions"
    gcs_uri   = f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}S1_transactions_core_banking.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="|",
        skip_leading_rows=1,      # ignorer la ligne d'en-tête
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = bq_client.get_table(table_id)
    log.info(f"✅ S1 chargé — {table.num_rows:,} lignes → {table_id}")


# ─────────────────────────────────────────────
#  S2 — CLIENTS KYC (JSONL)
# ─────────────────────────────────────────────
def ingest_s2_clients_kyc():
    """
    Format  : JSONL (JSON Lines — 1 objet JSON par ligne)
    Méthode : Chargement natif BigQuery depuis Cloud Storage
    Pourquoi: JSONL est supporté nativement par BigQuery
    """
    log.info("── S2 : Clients KYC ──")

    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.s2_clients_kyc"
    gcs_uri   = f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}S2_clients_kyc.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = bq_client.get_table(table_id)
    log.info(f"✅ S2 chargé — {table.num_rows:,} lignes → {table_id}")


# ─────────────────────────────────────────────
#  S3 — CONTRATS GUIDEWIRE (XML)
# ─────────────────────────────────────────────
def ingest_s3_contrats_xml():
    """
    Format  : XML hiérarchique (polices d'assurance avec balises imbriquées)
    Méthode : Python (xmltodict) → aplatissement → DataFrame → BigQuery
    Pourquoi: BigQuery ne supporte pas le XML nativement.
              L'XML est imbriqué (Police > Assure > Garanties) — il faut aplatir.
    """
    log.info("── S3 : Contrats Guidewire (XML) ──")

    raw = read_gcs_file(f"{GCS_PREFIX}S3_contrats_guidewire.xml")
    data = xmltodict.parse(raw.decode("utf-8"))

    polices = data["PolicesCentre"]["Police"]
    if isinstance(polices, dict):      # cas d'un seul enregistrement
        polices = [polices]

    rows = []
    for police in polices:
        # Extraire les garanties actives (liste ou dict unique)
        garanties = police.get("Garanties", {}).get("Garantie", [])
        if isinstance(garanties, dict):
            garanties = [garanties]
        garanties_actives = [
            g.get("@code", "") for g in garanties if g.get("@actif") == "true"
        ]
        assure = police.get("Assure", {})

        rows.append({
            "police_id"        : police.get("@id"),
            "version"          : police.get("@version"),
            "numero_police"    : police.get("NumeroPolice"),
            "code_produit"     : police.get("CodeProduit"),
            # Certaines polices ont PrimeTTC, d'autres MontantPrime
            "prime_ttc"        : float(police.get("PrimeTTC") or police.get("MontantPrime") or 0),
            "date_debut"       : police.get("DateDebut"),
            "date_fin"         : police.get("DateFin"),
            "assure_nom"       : assure.get("NOM"),
            "assure_prenom"    : assure.get("PRENOM"),
            "code_client"      : assure.get("CodeClient"),
            "garanties_actives": ",".join(garanties_actives),
            "nb_garanties"     : len(garanties_actives),
        })

    df = pd.DataFrame(rows)
    load_df_to_bq(df, "s3_contrats_guidewire")


# ─────────────────────────────────────────────
#  S4 — VIREMENTS SWIFT MT103 (texte propriétaire)
# ─────────────────────────────────────────────
def ingest_s4_swift_mt103():
    """
    Format  : SWIFT MT103 — format bancaire international (balises {:20:, :32A:…})
    Méthode : Parser regex Python → DataFrame → BigQuery
    Pourquoi: Format propriétaire non supporté par BigQuery ni Pandas.
              Chaque message est délimité par ---END--- et contient des balises SWIFT.
    """
    log.info("── S4 : Virements SWIFT MT103 ──")

    raw  = read_gcs_file(f"{GCS_PREFIX}S4_swift_mt103.txt")
    text = raw.decode("utf-8")

    # Découper en messages individuels séparés par ---END---
    messages = [m.strip() for m in text.split("---END---") if m.strip()]

    def extract_field(msg: str, tag: str) -> str:
        """Extrait la valeur d'une balise SWIFT (ex: :32A:)."""
        pattern = rf":{re.escape(tag)}:(.*?)(?=:\d{{2}}[A-Z]?:|$)"
        match = re.search(pattern, msg, re.DOTALL)
        return match.group(1).strip() if match else None

    rows = []
    for msg in messages:
        # :32A: → date + devise + montant (ex: "240309EUR297187,12")
        field_32a = extract_field(msg, "32A") or ""
        date_val  = field_32a[:6]  if len(field_32a) >= 6  else None
        devise    = field_32a[6:9] if len(field_32a) >= 9  else None
        montant_str = field_32a[9:].replace(",", ".") if len(field_32a) > 9 else "0"

        # :50K: → émetteur (2 lignes : IBAN puis nom)
        field_50k = (extract_field(msg, "50K") or "").split("\n")
        iban_emetteur = field_50k[0].lstrip("/") if field_50k else None
        nom_emetteur  = field_50k[1].strip()     if len(field_50k) > 1 else None

        # :59: → bénéficiaire
        field_59 = (extract_field(msg, "59") or "").split("\n")
        iban_benef = field_59[0].lstrip("/") if field_59 else None
        nom_benef  = field_59[1].strip()     if len(field_59) > 1 else None

        rows.append({
            "reference"      : extract_field(msg, "20"),
            "date_valeur"    : date_val,
            "devise"         : devise,
            "montant"        : float(montant_str) if montant_str else None,
            "iban_emetteur"  : iban_emetteur,
            "nom_emetteur"   : nom_emetteur,
            "banque_corresp" : extract_field(msg, "57A"),
            "iban_beneficiaire": iban_benef,
            "nom_beneficiaire" : nom_benef,
            "motif"          : extract_field(msg, "70"),
            "frais"          : extract_field(msg, "71A"),
        })

    df = pd.DataFrame(rows).dropna(subset=["reference"])
    load_df_to_bq(df, "s4_swift_mt103")


# ─────────────────────────────────────────────
#  S5 — SINISTRES AS/400 (Fixed-Width)
# ─────────────────────────────────────────────
def ingest_s5_sinistres_fixedwidth():
    """
    Format  : Fichier à largeur fixe (Fixed-Width) — legacy AS/400
    Méthode : pandas.read_fwf() avec positions de colonnes → BigQuery
    Pourquoi: Pas de séparateur — les colonnes sont définies par leur position
              de caractère. Format typique des anciens systèmes mainframe.

    Colonnes identifiées depuis la ligne d'en-tête :
    ID_SINISTRE(10) | STATUT(2) | DATE_SIN(8) | DATE_CLO(8) |
    MONTANT(18) | CODE_POLICE(12) | EXPERT(6) | LIBELLE(42)
    """
    log.info("── S5 : Sinistres AS/400 (Fixed-Width) ──")

    raw = read_gcs_file(f"{GCS_PREFIX}S5_sinistres_as400_fixedwidth.txt")

    # Définition des colonnes par position (start, end)
    colspecs = [
        (0,  10),   # ID_SINISTRE
        (10, 12),   # STATUT (OV=Ouvert, CL=Clos, SU=Suspendu, XX=Annulé)
        (12, 20),   # DATE_SINISTRE (YYYYMMDD)
        (20, 28),   # DATE_CLOTURE  (YYYYMMDD)
        (28, 46),   # MONTANT_INDEMNISATION (centimes)
        (46, 58),   # CODE_POLICE
        (58, 64),   # CODE_EXPERT
        (64, 106),  # LIBELLE_SINISTRE
    ]
    col_names = [
        "id_sinistre", "statut", "date_sinistre", "date_cloture",
        "montant_centimes", "code_police", "code_expert", "libelle_sinistre"
    ]

    df = pd.read_fwf(
        io.BytesIO(raw),
        colspecs=col_names and colspecs,
        names=col_names,
        skiprows=1,         # ignorer la ligne d'en-tête
        encoding="utf-8",
    )

    # Nettoyages
    df["montant_indemnisation"] = pd.to_numeric(
        df["montant_centimes"], errors="coerce"
    ) / 100  # conversion centimes → euros

    df["date_sinistre"] = pd.to_datetime(df["date_sinistre"], format="%Y%m%d", errors="coerce")
    df["date_cloture"]  = pd.to_datetime(df["date_cloture"],  format="%Y%m%d", errors="coerce")
    df["libelle_sinistre"] = df["libelle_sinistre"].str.strip()
    df = df.drop(columns=["montant_centimes"])

    load_df_to_bq(df, "s5_sinistres_as400")


# ─────────────────────────────────────────────
#  S6 — COURS BLOOMBERG (CSV classique)
# ─────────────────────────────────────────────
def ingest_s6_bloomberg():
    """
    Format  : CSV standard
    Méthode : Chargement natif BigQuery depuis Cloud Storage
    Pourquoi: Gros fichier (~50 MB) — le chargement natif BQ est plus rapide
              que passer par Python/Pandas.
    """
    log.info("── S6 : Cours Bloomberg ──")

    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.s6_bloomberg_ticks"
    gcs_uri   = f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}S6_marche_bloomberg_ticks.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = bq_client.get_table(table_id)
    log.info(f"✅ S6 chargé — {table.num_rows:,} lignes → {table_id}")


# ─────────────────────────────────────────────
#  S7 — AGENCES COMMERCIALES (Excel — 3 onglets)
# ─────────────────────────────────────────────
def ingest_s7_agences_excel():
    """
    Format  : Excel (.xlsx) avec 3 onglets
    Méthode : openpyxl via pandas.read_excel() → 3 tables BigQuery
    Pourquoi: BigQuery ne supporte pas le format Excel nativement.
              Chaque onglet devient une table distincte dans BigQuery.
    """
    log.info("── S7 : Agences Commerciales (Excel) ──")

    raw = read_gcs_file(f"{GCS_PREFIX}S7_agences_commerciales.xlsx")
    excel = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")

    log.info(f"Onglets trouvés : {excel.sheet_names}")

    for sheet_name in excel.sheet_names:
        df = pd.read_excel(excel, sheet_name=sheet_name, dtype=str)  # ← dtype=str ajouté

        # Remplacer les NaN (cellules vides) par des chaînes vides
        df = df.fillna("")

        # Nettoyage des noms de colonnes
        df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_", regex=False)
                  .str.replace(r"[^a-z0-9_]", "", regex=True)
        )

        table_name = "s7_" + sheet_name.lower().replace(" ", "_")
        load_df_to_bq(df, table_name)


# ─────────────────────────────────────────────
#  S8 — SCORING AML (JSONL)
# ─────────────────────────────────────────────
def ingest_s8_scoring_aml():
    """
    Format  : JSONL — JSON Lines (données sensibles AML/conformité)
    Méthode : Chargement natif BigQuery depuis Cloud Storage
    RGPD    : ⚠️ Table à accès restreint — ajouter des policies IAM après ingestion
    """
    log.info("── S8 : Scoring AML ──")

    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.s8_scoring_aml"
    gcs_uri   = f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}S8_scoring_aml.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = bq_client.get_table(table_id)
    log.info(f"✅ S8 chargé — {table.num_rows:,} lignes → {table_id}")
    log.warning("⚠️  S8 contient des données sensibles AML — restreindre l'accès IAM !")


# ─────────────────────────────────────────────
#  S9 — REPORTING ACPR COREP (CSV séparateur ;)
# ─────────────────────────────────────────────
def ingest_s9_acpr():
    """
    Format  : CSV avec séparateur ; (point-virgule)
    Méthode : Python → BigQuery (petit fichier, <1 MB)
    """
    log.info("── S9 : Reporting ACPR COREP ──")

    raw = read_gcs_file(f"{GCS_PREFIX}S9_reporting_acpr_corep.csv")
    df  = pd.read_csv(io.BytesIO(raw), sep=";", encoding="utf-8")

    # Nettoyage colonnes
    df.columns = (
        df.columns.str.strip().str.lower()
                  .str.replace(" ", "_", regex=False)
                  .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    load_df_to_bq(df, "s9_acpr_corep")


# ─────────────────────────────────────────────
#  S10 — LOGS DIGITAUX (JSONL — gros fichier)
# ─────────────────────────────────────────────
def ingest_s10_logs_digitaux():
    """
    Format  : JSONL — ~515 000 entrées (~75 MB)
    Méthode : Chargement natif BigQuery depuis Cloud Storage
    Pourquoi: Volume important → chargement natif BQ plus efficace que Pandas
    """
    log.info("── S10 : Logs Digitaux ──")

    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.s10_logs_digitaux"
    gcs_uri   = f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}S10_logs_digitaux.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    table = bq_client.get_table(table_id)
    log.info(f"✅ S10 chargé — {table.num_rows:,} lignes → {table_id}")


# ─────────────────────────────────────────────
#  POINT D'ENTRÉE PRINCIPAL
# ─────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  INGESTION CLOUD STORAGE → BIGQUERY")
    log.info("  Projet : Randstad Digital — Plateforme Data Cloud")
    log.info("=" * 60)

    # Créer le dataset BigQuery cible
    ensure_dataset(BQ_DATASET_RAW)

    # Exécuter chaque ingestion dans l'ordre
    # (Commencer par les fichiers accessibles sans LFS)
    ingestions = [
        ("S3 — Contrats XML",      ingest_s3_contrats_xml),
        ("S4 — SWIFT MT103",       ingest_s4_swift_mt103),
        ("S5 — Sinistres AS/400",  ingest_s5_sinistres_fixedwidth),
        ("S7 — Agences Excel",     ingest_s7_agences_excel),
        ("S9 — ACPR COREP",        ingest_s9_acpr),
        # Fichiers Git LFS — disponibles après git lfs pull :
        ("S1 — Transactions CSV",  ingest_s1_transactions),
        ("S2 — Clients KYC",       ingest_s2_clients_kyc),
        ("S6 — Bloomberg Ticks",   ingest_s6_bloomberg),
        ("S8 — Scoring AML",       ingest_s8_scoring_aml),
        ("S10 — Logs Digitaux",    ingest_s10_logs_digitaux),
    ]

    success, errors = [], []
    for label, fn in ingestions:
        try:
            fn()
            success.append(label)
        except Exception as e:
            log.error(f"❌ Échec {label} : {e}")
            errors.append((label, str(e)))

    # Rapport final
    log.info("=" * 60)
    log.info(f"  RÉSULTAT : {len(success)}/{len(ingestions)} sources chargées")
    if errors:
        log.error("  ÉCHECS :")
        for label, err in errors:
            log.error(f"    - {label} : {err}")
    log.info("=" * 60)
    log.info("  Prochaine étape : lancer les scripts SQL de la couche Staging")
    log.info("  Dataset cible : " + BQ_DATASET_RAW)


if __name__ == "__main__":
    main()
