# DONNEES SYNTHETIQUES - Secteur Bancaire/Assurance France
# Volume entreprise - Compatible GCP Free Tier
# Jean-Yves KPANGBAN | Data Engineer

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
2. S1,S5,S6,S9 -> Cloud Data Fusion (CSV/Fixed-Width)
3. S2,S8,S10   -> Cloud Data Fusion ou Dataflow (JSONL)
4. S3           -> Dataproc Spark (XML distribue)
5. S4           -> Cloud Functions (parser SWIFT custom)
6. S7           -> Cloud Functions (pre-traitement xlsx)
