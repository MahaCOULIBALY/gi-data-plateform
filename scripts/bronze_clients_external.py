"""bronze_clients_external.py — Enrichissement clients depuis SIRENE API + Salesforce.
Phase 1 · GI Data Lakehouse · Manifeste v2.0
# CORRECTIONS (2026-03-23) :
#   s3_delete_prefix centralisée depuis shared.py — purge avant écriture généralisée
#   run_sirene / run_salesforce : purge du préfixe date avant upload (idempotence re-run)
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


def generate_salesforce_stub() -> list[dict]:
    return [
        {"Account_Id": f"SF-{i:04d}", "Name": f"Client Test {i}", "Industry": "Staffing",
         "SIRET__c": f"1234567890{i:04d}", "BillingCity": "Paris", "CreatedDate": "2025-01-15"}
        for i in range(1, 6)
    ]


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
    batch_id = generate_batch_id()
    prefix = f"raw_salesforce_accounts/{today_s3_prefix()}"

    # Purge avant écriture — idempotence re-run dans la même journée
    # s3_delete_prefix gère le guard OFFLINE/PROBE : no-op hors mode LIVE
    s3_delete_prefix(cfg, cfg.bucket_bronze, prefix)

    if ext.salesforce_enabled:
        logger.info(json.dumps(
            {"info": "salesforce live mode not yet implemented — using stub"}))
        stats.warnings.append("Salesforce live mode not yet implemented")
    accounts = generate_salesforce_stub()
    enriched = [{**a, "_loaded_at": datetime.now(timezone.utc).isoformat(), "_batch_id": batch_id}
                for a in accounts]
    key = f"{prefix}/batch_{batch_id}.json"
    upload_to_s3(cfg, enriched, cfg.bucket_bronze, key, stats)
    stats.rows_ingested += len(enriched)
    stats.extra["salesforce_accounts"] = len(enriched)
    stats.tables_processed += 1


def run(cfg: Config) -> dict:
    stats = Stats()
    ext = ExternalConfig()
    run_sirene(cfg, ext, stats)
    run_salesforce(cfg, ext, stats)
    return stats.finish(cfg, PIPELINE)


if __name__ == "__main__":
    run(Config())
