#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — GI Data Lakehouse · Orchestrateur complet
# =============================================================================
# Usage :
#   ./run_pipeline.sh purge-bronze-probe      # Voir ce qui serait supprimé (Bronze S3 + watermarks)
#   ./run_pipeline.sh purge-bronze            # Purge Bronze réelle (S3 + watermarks)
#   ./run_pipeline.sh bronze-probe            # Sonder Evolia (compte, ne copie pas)
#   ./run_pipeline.sh bronze                  # Ingérer Bronze depuis Evolia
#   ./run_pipeline.sh silver-probe            # Probe Silver full-history (toutes partitions Bronze)
#   ./run_pipeline.sh silver                  # Transformer Silver incrémental (partition du jour)
#   ./run_pipeline.sh gold-probe              # Probe Gold (comptes + validation PG, pas d'écriture)
#   ./run_pipeline.sh gold                    # Écrire Gold dans PostgreSQL 18
#   ./run_pipeline.sh full-probe              # Probe de bout en bout (Bronze → Silver → Gold)
#   ./run_pipeline.sh full                    # Pipeline complet Bronze → Silver → Gold
#
# Variables d'environnement :
#   SILVER_DATE_PARTITION   Partition Silver (défaut : date du jour YYYY/MM/DD)
#                           Passer "**" pour full-history (1er run ou après purge Bronze)
#   FAIL_FAST=1             Arrêter à la première erreur (défaut : continuer)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Couleurs ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

# ── Helpers ───────────────────────────────────────────────────────────────────
_banner() { echo -e "\n${BOLD}${CYAN}════════════════════════════════════════${RESET}"; echo -e "${BOLD}${CYAN}  $1${RESET}"; echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"; }
_step()   { echo -e "\n${BOLD}── $1 ──${RESET}"; }
_ok()     { echo -e "${GREEN}✓ $1${RESET}"; }
_warn()   { echo -e "${YELLOW}⚠ $1${RESET}"; }
_err()    { echo -e "${RED}✗ $1${RESET}"; }
_fatal()  { _err "$1"; exit 1; }

ERRORS=()
_run() {
    local label="$1"; shift
    _step "$label"
    if "$@"; then
        _ok "$label"
        return 0
    else
        local rc=$?
        _err "$label → exit $rc"
        ERRORS+=("$label")
        [[ "${FAIL_FAST:-0}" == "1" ]] && _fatal "FAIL_FAST activé — arrêt."
        return $rc
    fi
}

_summary() {
    echo ""
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        echo -e "${GREEN}${BOLD}✓ Pipeline terminé sans erreur${RESET}"
    else
        echo -e "${RED}${BOLD}✗ ${#ERRORS[@]} erreur(s) :${RESET}"
        for e in "${ERRORS[@]}"; do echo -e "  ${RED}• $e${RESET}"; done
        exit 1
    fi
}

# ── Commande ──────────────────────────────────────────────────────────────────
CMD="${1:-help}"

# =============================================================================
# PURGE BRONZE — probe
# =============================================================================
if [[ "$CMD" == "purge-bronze-probe" ]]; then
    _banner "PURGE BRONZE — PROBE (aucune suppression)"
    _run "Purge Bronze S3+watermarks probe" \
        bash -c "RUN_MODE=probe uv run scripts/purge_bronze.py --target all"
    _summary

# =============================================================================
# PURGE BRONZE — live
# =============================================================================
elif [[ "$CMD" == "purge-bronze" ]]; then
    _banner "PURGE BRONZE — LIVE (suppression irréversible)"
    _warn "Cette opération supprime TOUS les objets S3 Bronze + watermarks PostgreSQL."
    read -rp "Confirmer ? [oui/non] " confirm
    [[ "$confirm" == "oui" ]] || _fatal "Annulé."

    _run "Purge Bronze S3 + watermarks" \
        bash -c "RUN_MODE=live uv run scripts/purge_bronze.py --target all"
    _summary

# =============================================================================
# PURGE SILVER — probe
# =============================================================================
elif [[ "$CMD" == "purge-silver-probe" ]]; then
    _banner "PURGE SILVER — PROBE (aucune suppression)"
    _run "Purge Silver S3 probe" \
        bash -c "RUN_MODE=probe uv run scripts/purge_silver.py"
    _summary

# =============================================================================
# PURGE SILVER — live
# =============================================================================
elif [[ "$CMD" == "purge-silver" ]]; then
    _banner "PURGE SILVER — LIVE (suppression irréversible)"
    _warn "Cette opération supprime TOUS les Parquet S3 Silver (gi-data-prod-silver)."
    read -rp "Confirmer ? [oui/non] " confirm
    [[ "$confirm" == "oui" ]] || _fatal "Annulé."

    _run "Purge Silver S3" \
        bash -c "RUN_MODE=live uv run scripts/purge_silver.py"
    _summary

# =============================================================================
# PURGE GOLD — probe
# =============================================================================
elif [[ "$CMD" == "purge-gold-probe" ]]; then
    _banner "PURGE GOLD — PROBE (aucune suppression)"
    _run "Purge Gold PostgreSQL probe" \
        bash -c "RUN_MODE=probe uv run scripts/purge_gold.py"
    _summary

# =============================================================================
# PURGE GOLD — live
# =============================================================================
elif [[ "$CMD" == "purge-gold" ]]; then
    _banner "PURGE GOLD — LIVE (TRUNCATE toutes les tables gld_*)"
    _warn "Cette opération vide TOUTES les tables Gold PostgreSQL (TRUNCATE CASCADE)."
    read -rp "Confirmer ? [oui/non] " confirm
    [[ "$confirm" == "oui" ]] || _fatal "Annulé."

    _run "Purge Gold PostgreSQL" \
        bash -c "RUN_MODE=live uv run scripts/purge_gold.py"
    _summary

# =============================================================================
# PURGE ALL — probe (Bronze + Silver + Gold)
# =============================================================================
elif [[ "$CMD" == "purge-all-probe" ]]; then
    _banner "PURGE ALL — PROBE (Bronze + Silver + Gold, aucune suppression)"
    _run "Purge Bronze S3+watermarks probe" bash -c "RUN_MODE=probe uv run scripts/purge_bronze.py --target all"
    _run "Purge Silver S3 probe"            bash -c "RUN_MODE=probe uv run scripts/purge_silver.py"
    _run "Purge Gold PostgreSQL probe"      bash -c "RUN_MODE=probe uv run scripts/purge_gold.py"
    _summary

# =============================================================================
# PURGE ALL — live (Bronze + Silver + Gold)
# =============================================================================
elif [[ "$CMD" == "purge-all" ]]; then
    _banner "PURGE ALL — LIVE (Bronze + Silver + Gold, irréversible)"
    _warn "Cette opération supprime TOUTES les données de toutes les couches."
    _warn "Bronze S3 + watermarks | Silver S3 | Gold PostgreSQL TRUNCATE"
    read -rp "Confirmer ? [oui/non] " confirm
    [[ "$confirm" == "oui" ]] || _fatal "Annulé."

    _run "Purge Bronze S3 + watermarks" bash -c "RUN_MODE=live uv run scripts/purge_bronze.py --target all"
    _run "Purge Silver S3"              bash -c "RUN_MODE=live uv run scripts/purge_silver.py"
    _run "Purge Gold PostgreSQL"        bash -c "RUN_MODE=live uv run scripts/purge_gold.py"
    _summary

# =============================================================================
# BRONZE — probe
# =============================================================================
elif [[ "$CMD" == "bronze-probe" ]]; then
    _banner "BRONZE — PROBE (compte Evolia, zéro écriture S3)"

    _run "bronze_agences"           bash -c "RUN_MODE=probe uv run scripts/bronze_agences.py"
    _run "bronze_clients"           bash -c "RUN_MODE=probe uv run scripts/bronze_clients.py"
    _run "bronze_interimaires"      bash -c "RUN_MODE=probe uv run scripts/bronze_interimaires.py"
    _run "bronze_missions"          bash -c "RUN_MODE=probe uv run scripts/bronze_missions.py"
    _run "bronze_clients_external"  bash -c "RUN_MODE=probe uv run scripts/bronze_clients_external.py"

    _summary

# =============================================================================
# BRONZE — live
# =============================================================================
elif [[ "$CMD" == "bronze" ]]; then
    _banner "BRONZE — LIVE (ingestion Evolia → S3 JSON)"

    # Les 4 sources principales sont indépendantes — ordre quelconque
    _run "bronze_agences"           bash -c "RUN_MODE=live uv run scripts/bronze_agences.py"
    _run "bronze_clients"           bash -c "RUN_MODE=live uv run scripts/bronze_clients.py"
    _run "bronze_interimaires"      bash -c "RUN_MODE=live uv run scripts/bronze_interimaires.py"
    _run "bronze_missions"          bash -c "RUN_MODE=live uv run scripts/bronze_missions.py"
    # External optionnel (SIRENE/Salesforce) — non-bloquant
    _run "bronze_clients_external"  bash -c "RUN_MODE=live uv run scripts/bronze_clients_external.py" || true

    _summary

# =============================================================================
# SILVER — probe full-history (toutes les partitions Bronze)
# =============================================================================
elif [[ "$CMD" == "silver-probe" ]]; then
    _banner "SILVER — PROBE full-history (SILVER_DATE_PARTITION=**)"
    export SILVER_DATE_PARTITION="${SILVER_DATE_PARTITION:-**}"
    export RUN_MODE=probe
    echo -e "  SILVER_DATE_PARTITION=${SILVER_DATE_PARTITION}"

    _run "silver_agences_light"      bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_agences_light.py"
    _run "silver_clients"            bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_clients.py"
    _run "silver_clients_detail"     bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_clients_detail.py"
    _run "silver_interimaires"       bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_interimaires.py"
    _run "silver_competences"        bash -c "RUN_MODE=probe uv run scripts/silver_competences.py"
    _run "silver_interimaires_detail" bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_interimaires_detail.py"
    _run "silver_missions"           bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_missions.py"
    _run "silver_temps"              bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_temps.py"
    _run "silver_factures"           bash -c "RUN_MODE=probe SILVER_DATE_PARTITION=$SILVER_DATE_PARTITION uv run scripts/silver_factures.py"

    _summary

# =============================================================================
# SILVER — live incrémental (partition du jour ou SILVER_DATE_PARTITION forcé)
# =============================================================================
elif [[ "$CMD" == "silver" ]]; then
    # SILVER_DATE_PARTITION par défaut = date du jour (run quotidien après Bronze du jour)
    # Pour un run full-history après purge Bronze : SILVER_DATE_PARTITION='**' ./run_pipeline.sh silver
    PART="${SILVER_DATE_PARTITION:-$(date -u +%Y/%m/%d)}"
    _banner "SILVER — LIVE (partition: ${PART})"
    echo -e "  SILVER_DATE_PARTITION=${PART}"

    _run "silver_agences_light"      bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_agences_light.py"
    _run "silver_clients"            bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_clients.py"
    _run "silver_clients_detail"     bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_clients_detail.py"
    _run "silver_interimaires"       bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_interimaires.py"
    _run "silver_competences"        bash -c "RUN_MODE=live uv run scripts/silver_competences.py"
    _run "silver_interimaires_detail" bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_interimaires_detail.py"
    _run "silver_missions"           bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_missions.py"
    _run "silver_temps"              bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_temps.py"
    _run "silver_factures"           bash -c "RUN_MODE=live SILVER_DATE_PARTITION=$PART uv run scripts/silver_factures.py"

    _summary

# =============================================================================
# GOLD — probe
# =============================================================================
elif [[ "$CMD" == "gold-probe" ]]; then
    _banner "GOLD — PROBE"

    _run "probe_pg (connectivité PostgreSQL)"  bash -c "RUN_MODE=probe uv run scripts/probe_pg.py"

    for script in gold_dimensions gold_ca_mensuel gold_clients_detail \
                  gold_competences gold_etp gold_operationnel gold_recouvrement \
                  gold_qualite_missions gold_retention_client gold_scorecard_agence \
                  gold_staffing gold_vue360_client; do
        _run "$script" bash -c "RUN_MODE=probe uv run scripts/${script}.py" || true
    done

    _summary

# =============================================================================
# GOLD — live
# =============================================================================
elif [[ "$CMD" == "gold" ]]; then
    _banner "GOLD — LIVE"

    for script in gold_dimensions gold_ca_mensuel gold_clients_detail \
                  gold_competences gold_etp gold_operationnel gold_recouvrement \
                  gold_qualite_missions gold_retention_client gold_scorecard_agence \
                  gold_staffing gold_vue360_client; do
        _run "$script" bash -c "RUN_MODE=live uv run scripts/${script}.py"
    done

    _summary

# =============================================================================
# PIPELINE COMPLET — probe
# =============================================================================
elif [[ "$CMD" == "full-probe" ]]; then
    _banner "PIPELINE COMPLET — PROBE (Bronze → Silver → Gold)"
    export SILVER_DATE_PARTITION="${SILVER_DATE_PARTITION:-**}"

    bash "${BASH_SOURCE[0]}" bronze-probe
    bash "${BASH_SOURCE[0]}" silver-probe
    bash "${BASH_SOURCE[0]}" gold-probe
    _summary

# =============================================================================
# PIPELINE COMPLET — live
# =============================================================================
elif [[ "$CMD" == "full" ]]; then
    _banner "PIPELINE COMPLET — LIVE"

    bash "${BASH_SOURCE[0]}" bronze
    SILVER_DATE_PARTITION="$(date -u +%Y/%m/%d)" bash "${BASH_SOURCE[0]}" silver
    bash "${BASH_SOURCE[0]}" gold
    _summary

# =============================================================================
# HELP
# =============================================================================
else
    echo -e "${BOLD}Usage :${RESET} ./run_pipeline.sh <commande>"
    echo ""
    echo -e "${BOLD}Purge :${RESET}"
    echo "  purge-all-probe       Probe toutes les couches (Bronze + Silver + Gold)"
    echo "  purge-all             Purge complète avec confirmation"
    echo "  purge-bronze-probe    Probe Bronze (S3 + watermarks)"
    echo "  purge-bronze          Purge Bronze avec confirmation"
    echo "  purge-silver-probe    Probe Silver (S3 Parquet)"
    echo "  purge-silver          Purge Silver avec confirmation"
    echo "  purge-gold-probe      Probe Gold (TRUNCATE gld_* PostgreSQL)"
    echo "  purge-gold            Purge Gold avec confirmation"
    echo ""
    echo -e "${BOLD}Bronze (Evolia → S3 JSON) :${RESET}"
    echo "  bronze-probe          Compte Evolia, zéro écriture"
    echo "  bronze                Ingestion complète"
    echo ""
    echo -e "${BOLD}Silver (Bronze S3 → Parquet S3) :${RESET}"
    echo "  silver-probe          Probe full-history (SILVER_DATE_PARTITION=**)"
    echo "  silver                Live incrémental (date du jour par défaut)"
    echo "  SILVER_DATE_PARTITION='**' ./run_pipeline.sh silver   # full-history"
    echo ""
    echo -e "${BOLD}Gold (Silver Parquet → PostgreSQL 18) :${RESET}"
    echo "  gold-probe            Probe connectivité + comptes"
    echo "  gold                  Silver Parquet → PostgreSQL Gold"
    echo ""
    echo -e "${BOLD}Pipeline complet :${RESET}"
    echo "  full-probe            Probe de bout en bout"
    echo "  full                  Bronze + Silver + Gold live"
    echo ""
    echo -e "${BOLD}Variables :${RESET}"
    echo "  SILVER_DATE_PARTITION='**'   Full-history (après purge Bronze)"
    echo "  FAIL_FAST=1                  Arrêt à la première erreur"
fi
