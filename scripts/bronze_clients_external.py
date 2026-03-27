"""bronze_clients_external.py — Enrichissement clients depuis SIRENE API + Salesforce.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS (2026-03-23) :
#   s3_delete_prefix centralisée depuis shared.py — purge avant écriture généralisée
#   run_sirene / run_salesforce : purge du préfixe date avant upload (idempotence re-run)
# SALESFORCE (2026-03-26) :
#   run_salesforce : stub remplacé par extraction réelle via simple_salesforce
#   Credentials : SF_USERNAME / SF_PASSWORD / SF_CONSUMER_KEY / SF_CONSUMER_SECRET / SF_DOMAIN
#   Préfixe S3 : raw_sf_accounts/ (clé de join silver_clients : Id_Evolia__c = TIE_ID)
#   Champs extraits : Id, Id_Evolia__c, SIREN__c, NIC__c, Code_NAF_Name,
#                     Statut_du_Compte__c, Date_de_derniere_facture__c, adresse livraison
"""
import os
import json
import time
import urllib.request
from datetime import datetime, timezone
from dataclasses import dataclass, field

from shared import (
    Config, Stats, generate_batch_id, today_s3_prefix,
    upload_to_s3, s3_delete_prefix, logger,
)
from pipeline_utils import with_retry

PIPELINE = "bronze_clients_external"
SIRENE_ENDPOINT = "https://api.insee.fr/entreprises/sirene/V3.11/siret"
SIRENE_RATE_LIMIT = 30  # req/min → 2s between requests


@dataclass
class ExternalConfig:
    sirene_token: str = field(
        default_factory=lambda: os.environ.get("SIRENE_API_TOKEN", ""))
    salesforce_enabled: bool = field(
        default_factory=lambda: os.environ.get("SALESFORCE_ENABLED", "0") == "1")
    sirets_file: str = field(default_factory=lambda: os.environ.get(
        "SIRETS_INPUT", "data/sirets_clients.json"))
    sf_username: str = field(default_factory=lambda: os.environ.get("SF_USERNAME", ""))
    sf_password: str = field(default_factory=lambda: os.environ.get("SF_PASSWORD", ""))
    sf_consumer_key: str = field(default_factory=lambda: os.environ.get("SF_CONSUMER_KEY", ""))
    sf_consumer_secret: str = field(default_factory=lambda: os.environ.get("SF_CONSUMER_SECRET", ""))
    sf_domain: str = field(default_factory=lambda: os.environ.get("SF_DOMAIN", "login"))


@with_retry(max_attempts=3, base_delay=2.0, backoff=2.0)
def fetch_sirene(siret: str, token: str) -> dict | None:
    """Appel API SIRENE INSEE pour un SIRET donné. Retourne None si 404."""
    url = f"{SIRENE_ENDPOINT}/{siret}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
        etab = data.get("etablissement", {})
        unite = etab.get("uniteLegale", {})
        adresse = etab.get("adresseEtablissement", {})
        return {
            "siret": siret,
            "raison_sociale_officielle": unite.get("denominationUniteLegale", ""),
            "date_creation": unite.get("dateCreationUniteLegale", ""),
            "tranche_effectif": etab.get("trancheEffectifsEtablissement", ""),
            "naf": etab.get("activitePrincipaleEtablissement", ""),
            "adresse_siege": " ".join(filter(None, [
                adresse.get("numeroVoieEtablissement", ""),
                adresse.get("typeVoieEtablissement", ""),
                adresse.get("libelleVoieEtablissement", ""),
                adresse.get("codePostalEtablissement", ""),
                adresse.get("libelleCommuneEtablissement", ""),
            ])),
        }


def load_sirets(path: str) -> list[str]:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(json.dumps(
            {"warning": "sirets_file_not_found", "path": path}))
        return []


_SF_SOQL = """
    SELECT Id, Id_Wizim__c, Id_Evolia__c, Matricule_Evolia__c, Name,
           SIREN__c, NIC__c, SIRETFormula__c,
           ShippingStreet, ShippingPostalCode, ShippingCity, Code_Insee__c,
           Date_de_derniere_facture__c, Statut_du_Compte__c, CreatedDate, CreatedBy.Name,
           ESCONNECT__TVA__c, Code_NAF__r.name, ESCONNECT__FORMULA_companyStatus__c
    FROM Account
    WHERE RecordType.DeveloperName = 'Etablissement'
"""


def fetch_sf_accounts(ext: "ExternalConfig") -> list[dict]:
    """Extraction Salesforce Accounts (RecordType=Etablissement) via simple_salesforce.
    Aplatit les objets imbriqués CreatedBy et Code_NAF__r.
    Normalise ShippingStreet (suppression retours à la ligne).
    """
    from simple_salesforce import Salesforce  # import local — dépendance optionnelle
    sf = Salesforce(
        username=ext.sf_username,
        password=ext.sf_password,
        consumer_key=ext.sf_consumer_key,
        consumer_secret=ext.sf_consumer_secret,
        domain=ext.sf_domain,
        version="62.0",
    )
    result = sf.query_all(_SF_SOQL)
    records = []
    for rec in result["records"]:
        row = {k: v for k, v in rec.items() if k != "attributes"}
        created_by = row.pop("CreatedBy", None)
        row["CreatedBy_Name"] = created_by.get("Name") if isinstance(created_by, dict) else None
        code_naf = row.pop("Code_NAF__r", None)
        row["Code_NAF_Name"] = code_naf.get("Name") if isinstance(code_naf, dict) else None
        if isinstance(row.get("ShippingStreet"), str):
            row["ShippingStreet"] = row["ShippingStreet"].replace("\n", " ").replace("\r", " ").strip()
        records.append(row)
    return records


def run_sirene(cfg: Config, ext: ExternalConfig, stats: Stats) -> None:
    if not ext.sirene_token:
        logger.warning(json.dumps(
            {"warning": "SIRENE_API_TOKEN not set — skipping"}))
        stats.warnings.append("SIRENE_API_TOKEN missing")
        return
    sirets = load_sirets(ext.sirets_file)
    if not sirets:
        return
    batch_id = generate_batch_id()
    prefix = f"raw_sirene/{today_s3_prefix()}"

    # Purge avant écriture — idempotence re-run dans la même journée
    # s3_delete_prefix gère le guard OFFLINE/PROBE : no-op hors mode LIVE
    s3_delete_prefix(cfg, cfg.bucket_bronze, prefix)

    results, not_found = [], 0
    for i, siret in enumerate(sirets):
        try:
            data = fetch_sirene(siret.strip(), ext.sirene_token)
            if data:
                data.update({"_loaded_at": datetime.now(timezone.utc).isoformat(),
                             "_batch_id": batch_id})
                results.append(data)
            else:
                not_found += 1
        except urllib.error.HTTPError as e:
            if e.code == 404:
                not_found += 1
            else:
                logger.exception(json.dumps({"siret": siret, "error": str(e)}))
                stats.errors.append({"siret": siret, "error": str(e)})
        except Exception as e:
            logger.exception(json.dumps({"siret": siret, "error": str(e)}))
            stats.errors.append({"siret": siret, "error": str(e)})
        time.sleep(2 if (i + 1) % SIRENE_RATE_LIMIT != 0 else 60)

    if results:
        key = f"{prefix}/batch_{batch_id}.json"
        upload_to_s3(cfg, results, cfg.bucket_bronze, key, stats)
        stats.rows_ingested += len(results)
        stats.extra.update({"sirene_found": len(results),
                           "sirene_not_found": not_found})
        stats.tables_processed += 1


def run_salesforce(cfg: Config, ext: ExternalConfig, stats: Stats) -> None:
    if not ext.salesforce_enabled:
        logger.info(json.dumps({"info": "salesforce disabled (SALESFORCE_ENABLED=0) — skipping"}))
        return

    sf_creds_ok = all([ext.sf_username, ext.sf_password, ext.sf_consumer_key, ext.sf_consumer_secret])
    if not sf_creds_ok:
        logger.warning(json.dumps({"warning": "SF credentials missing — skipping salesforce"}))
        stats.warnings.append("SF credentials missing (SF_USERNAME/SF_PASSWORD/SF_CONSUMER_KEY/SF_CONSUMER_SECRET)")
        return

    batch_id = generate_batch_id()
    prefix = f"raw_sf_accounts/{today_s3_prefix()}"

    # Purge avant écriture — idempotence re-run dans la même journée
    # s3_delete_prefix gère le guard OFFLINE/PROBE : no-op hors mode LIVE
    s3_delete_prefix(cfg, cfg.bucket_bronze, prefix)

    try:
        accounts = fetch_sf_accounts(ext)
    except Exception as e:
        logger.exception(json.dumps({"salesforce": "error", "error": str(e)}))
        stats.errors.append({"source": "salesforce", "error": str(e)})
        return

    enriched = [
        {**a, "_loaded_at": datetime.now(timezone.utc).isoformat(), "_batch_id": batch_id}
        for a in accounts
    ]
    key = f"{prefix}/batch_{batch_id}.json"
    upload_to_s3(cfg, enriched, cfg.bucket_bronze, key, stats)
    stats.rows_ingested += len(enriched)
    stats.extra["salesforce_accounts"] = len(enriched)
    stats.tables_processed += 1
    logger.info(json.dumps({"salesforce": "ok", "accounts": len(enriched)}))


def run(cfg: Config) -> dict:
    stats = Stats()
    ext = ExternalConfig()
    run_sirene(cfg, ext, stats)
    run_salesforce(cfg, ext, stats)
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
