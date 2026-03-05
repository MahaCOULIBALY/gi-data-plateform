"""probe_ddl_schema.py — Référentiel colonnes attendues par domaine métier.
Phase 0 · GI Data Lakehouse · Manifeste v2.0
Annotations :
  # ✓  colonne confirmée (DDL probe 2026-03-05 : présente + non-missing)
  # +  colonne ajoutée après probe (extra DDL, business-relevant)
  # ~  colonne inférée depuis architecture DDL (non réfutée)
  # x  colonne SUPPRIMÉE — absente DDL (était annotée ~, confirmée FAIL probe)
Fichier de données pure — aucune logique.

Rappel stratégie delta vs full-load :
  DELTA possible seulement si colonne *_DATEMODIF confirmée en DDL.
  Toutes les tables intérimaires + clients (sauf CMTIER, WTPEVAL, WTUGPINT) → FULL-LOAD.
"""

# ─────────────────────────────────────────────────────────────────
# DOMAINE : INTÉRIMAIRES
# ─────────────────────────────────────────────────────────────────

_PYPERSONNE = [
    "PER_ID",           # ✓ clé pivot universelle
    "PER_NOM",          # ✓ confirmé (non-missing probe)
    "PER_PRENOM",       # ✓
    "PER_NIR",          # ✓ SENSIBLE — pseudonymisé Silver
    "PER_CP",           # ✓
    "PER_VILLE",        # ✓
    "PER_NAISSANCE",    # + remplace PER_DATENAIS (x absent DDL)
    "NAT_CODE",         # + remplace PER_NATIONALITE (x absent DDL)
    "PAYS_CODE",        # + remplace PER_PAYS (x absent DDL)
    "PER_BISVOIE",      # + numéro+bis voie (remplace PER_NUMVOIE x)
    "PER_COMPVOIE",     # + complément voie (remplace PER_TYPVOIE/PER_VOIE x)
    "PER_COMMUNE",      # + code commune
    # x PER_SEXE         — absent DDL
    # x PER_LIEUNAIS     — absent DDL
    # x PER_ADRESSE      — absent DDL (scindé en BISVOIE/COMPVOIE)
    # x PER_COMPL        — absent DDL
    # x PER_DATEMODIF    — absent DDL → FULL-LOAD obligatoire
    # x PER_DATECREA     — absent DDL
    # x PER_ACTIF        — absent DDL
]

_PYSALARIE = [
    "PER_ID",           # ✓
    "SAL_MATRICULE",    # ✓ confirmé (non-missing probe)
    "SAL_ACTIF",        # ✓
    "SAL_DATEENTREE",   # + remplace SAL_DATEDEBUT (x absent DDL)
    # x SAL_DATEFIN      — absent DDL
    # x ETA_ID           — absent DDL (jointure via PYOSPETA)
    # x SAL_DATEMODIF    — absent DDL → FULL-LOAD
]

_WTPINT = [
    "PER_ID",           # ✓
    "PINT_CANDIDAT",    # ✓ confirmé (non-missing probe)
    "PINT_DOSSIER",     # ✓
    "PINT_PERMANENT",   # ✓
    # x PINT_PLACEMENT   — absent DDL
    # x RGPCNT_ID        — absent DDL dans WTPINT (agence via autre table)
    # x PINT_DATEMODIF   — absent DDL → FULL-LOAD
]

_PYCOORDONNEE = [
    "PER_ID",           # ✓
    "TYPTEL_CODE",      # + remplace TYPTEL (x absent DDL)
    "PER_TEL_NTEL",     # + remplace COORD_VALEUR (x absent DDL) — SENSIBLE
    "PER_TEL_POSTE",    # + poste téléphonique
    # x COORD_DATEMODIF  — absent DDL → FULL-LOAD
]

_WTPMET = [
    "PER_ID",           # ✓
    "PMET_ORDRE",       # + remplace ORDRE (x absent DDL)
    "MET_ID",           # ✓ confirmé (non-missing probe)
    # x PMET_NIVEAU      — absent DDL
    # x PMET_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTPHAB = [
    "PER_ID",           # ✓
    "THAB_ID",          # ✓
    "PHAB_DELIVR",      # + date délivrance (remplace PHAB_DATEDEB x)
    "PHAB_EXPIR",       # + date expiration (remplace PHAB_DATEFIN x)
    "PHAB_ORDRE",       # +
    # x PHAB_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTPDIP = [
    "PER_ID",           # ✓
    "TDIP_ID",          # ✓
    "PDIP_DATE",        # + remplace PDIP_ANNEE (x absent DDL)
    # x PDIP_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTEXP = [
    "PER_ID",           # ✓
    "EXP_ORDRE",        # + remplace ORDRE (x absent DDL)
    "EXP_NOM",          # + remplace EXP_SOCIETE (x absent DDL)
    "EXP_DEBUT",        # + remplace EXP_DATEDEB (x absent DDL)
    "EXP_FIN",          # + remplace EXP_DATEFIN (x absent DDL)
    "EXP_INTERNE",      # +
    # x EXP_POSTE        — absent DDL
    # x EXP_DATEMODIF    — absent DDL → FULL-LOAD
]

_WTPEVAL = [
    "PER_ID",           # ✓
    "PEVAL_DU",         # + delta col (remplace PEVAL_DATE x)
    "PEVAL_EVALUATION",  # + remplace PEVAL_NOTE (x absent DDL)
    "PEVAL_UTL",        # + remplace PEVAL_AGENT (x absent DDL)
    # x PEVAL_COMMENTAIRE — absent DDL
    # x RGPCNT_ID         — absent DDL
]

_RHPERSONNE = [
    "PER_ID",           # ✓
    # x RHP_DATEMODIF    — absent DDL → FULL-LOAD
    # x RHP_DATEVISITEMEDIC — absent DDL
    # x RHP_HANDICAP     — absent DDL (données dans RHPER_* extra)
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : CLIENTS
# ─────────────────────────────────────────────────────────────────

_WTTIESERV = [
    "TIE_ID",           # ✓
    "TIES_SERV",        # ✓
    "TIES_RAISOC",      # ✓ bronze _COLS confirmé
    "TIES_ADR1",        # ✓
    "TIES_ADR2",        # ✓
    "TIES_CODPOS",      # ✓
    "TIES_VILLE",       # ✓
    # x TIES_RS          — absent DDL (variante courte non présente)
    # x TIES_SIRET       — absent DDL
    # x TIES_NAF         — absent DDL (champ NAF absent de WTTIESERV)
    # x TIES_NUMVOIE/TYPVOIE/VOIE — absent DDL
    # x TIES_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTCLPT = [
    "TIE_ID",           # ✓
    "CLPT_PROSPEC",     # ✓ bronze (proxy statut — CLPT_PROSPECT x absent)
    "CLPT_CAESTIME",    # ✓ bronze (CA estimé — CLPT_CAPOT x absent)
    "CLPT_DATCREA",     # ✓ bronze (CLPT_DATECREA x absent)
    # x CLPT_ACTIF        — absent DDL
    # x CLPT_EFFECTIF     — absent DDL
    # x CLPT_DATEMODIF    — absent DDL → FULL-LOAD
]

_WTTIEINT = [
    "TIE_ID",           # ✓
    "TIEI_ORDRE",       # + remplace ORDRE (x absent DDL)
    "TIEI_NOM",         # + remplace TIEINT_NOM (x absent DDL)
    "TIEI_PRENOM",      # + remplace TIEINT_PRENOM
    "TIEI_EMAIL",       # + remplace TIEINT_EMAIL
    "TIEI_BUREAU",      # + numéro bureau (remplace TIEINT_TEL)
    "FCTI_CODE",        # + code fonction (remplace TIEINT_FONCTION)
    # x TIEINT_DATEMODIF  — absent DDL → FULL-LOAD
]

_WTCOEF = [
    "TQUA_ID",          # + remplace TQUA (x absent DDL)
    "RGPCNT",           # ✓ confirmé (non-missing probe)
    "TIE",              # ✓
    "COEF_VAL",         # ✓ bronze
    "COEF_DEFF",        # + remplace COEF_DATEDEB (x absent DDL)
    "COEF_DFIN",        # + remplace COEF_DATEFIN (x absent DDL)
    # x COEF_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTENCOURSG = [
    "ENCGRP_ID",        # ✓ clé (non-missing probe)
    "ENC_SIREN",        # + remplace SIREN (x absent DDL)
    "ENCG_DECISIONLIB",  # +
    # x ENCGRP_LIMITE/MONTANT/DATE/STATUT — absent DDL
    # x ENCGRP_DATEMODIF — absent DDL → FULL-LOAD
]

_WTUGCLI = [
    "RGPCNT_ID",        # ✓
    "TIE_ID",           # ✓
    "UGCLI_ORIG",       # + (extra DDL disponible)
    # x UGCLI_DATE/MONTANT — absent DDL → FULL-LOAD
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : AGENCES & STRUCTURE
# ─────────────────────────────────────────────────────────────────

_WTUG = [
    "RGPCNT_ID",        # ✓ FK pivot
    "UG_GPS",           # + champ GPS combiné (remplace UG_GPS_LAT/LNG x)
    "UG_CLOTURE_DATE",  # + remplace UG_CLOTURE (x absent DDL)
    "UG_CLOTURE_USER",  # +
    "PIL_ID",           # + remplace UG_PILOTE (x absent DDL)
    # x UG_NOM           — absent DDL (nom = PYREGROUPCNT.RGPCNT_LIBELLE)
    # x UG_DATEMODIF     — absent DDL → FULL-LOAD
]

_PYETABLISSEMENT = [
    "ETA_ID",           # ✓
    "ENT_ID",           # ✓
    "ETA_NIC",          # ✓ bronze
    "ETA_ADR1",         # ✓
    "ETA_ADR2",         # ✓
    "ETA_ADR2_COMP",    # +
    "ETA_CODPOS",       # ✓
    "ETA_VILLE",        # ✓
    "ETA_NAF",          # ✓
    "ETA_COMMUNE",      # ✓
    "ETA_ACTIVITE",     # ✓
    # x ETA_SIRET        — absent DDL
    # x ETA_DATEMODIF    — absent DDL → FULL-LOAD
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : MISSIONS & CONTRATS
# ─────────────────────────────────────────────────────────────────

_WTMISS = [
    "PER_ID",           # ✓
    "CNT_ID",           # ✓
    "TIE_ID",           # ✓
    "TIES_SERV",        # ✓ = MISS_TIEID dans bronze (alias)
    "RGPCNT_ID",        # ✓
    "CNTI_CREATE",      # ✓ delta col Bronze
    # ✓ remplace MISS_CODEFIN (WARN probe — MISS_CODEFIN absent)
    "FINMISS_CODE",
    "MISS_SAISIE_DTFIN",  # ✓ date fin saisie (proxy MISS_DATEFIN WARN)
    "MISS_JUSTIFICATION",  # ✓
    # ~ MISS_DATEDEBUT   — WARN probe (absent DDL confirmé)
    # ~ MISS_MOTIF        — WARN probe
    # ~ PRH_BTS           — WARN probe
]

_WTCNTI = [
    "PER_ID",           # ✓
    "CNT_ID",           # ✓
    "CNTI_ORDRE",       # + remplace ORDRE (x absent DDL)
    "MET_ID",           # ✓
    "PCS_CODE_2003",    # + remplace TPCI_CODE (x) — code PCS
    "CNTI_CREATE",      # ✓ delta col
    "CNTI_DATEFFET",    # + remplace CNT_DATEDEBUT (x)
    "CNTI_DATEFINCNTI",  # + remplace CNT_DATEFIN (x)
    "CNTI_THPAYE",      # + remplace CNT_TAUXPAYE (x)
    "CNTI_THFACT",      # + remplace CNT_TAUXFACT (x)
    "CNTI_DURHEBDO",    # + remplace CNT_NBHEURE (x)
    "CNTI_POSTE",       # + remplace CNT_POSTE (x)
    "LOTFAC_CODE",      # ✓
]

_WTCMD = [
    "CMD_ID",           # ✓
    "RGPCNT_ID",        # ✓
    "TIE_ID",           # ✓
    "MET_ID",           # ✓
    "CMD_DTE",          # + remplace CMD_DATE (x absent DDL)
    "CMD_NBSALS",       # + remplace CMD_NBSAL (x)
    # x CMD_STATUT       — absent DDL visible
    # x CMD_DATEMODIF    — absent DDL → FULL-LOAD
]

_WTPLAC = [
    "PLAC_ID",          # ✓
    "RGPCNT_ID",        # ✓
    "TIE_ID",           # ✓
    "MET_ID",           # ✓
    "PLAC_DTEEDI",      # + remplace PLAC_DATE (x absent DDL)
    # x PLAC_STATUT      — absent DDL visible
    # x PLAC_DATEMODIF   — absent DDL → FULL-LOAD
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : FACTURATION
# ─────────────────────────────────────────────────────────────────

_WTEFAC = [
    "EFAC_NUM",         # ✓
    "RGPCNT_ID",        # ✓
    "TIE_ID",           # ✓
    "TIES_SERV",        # ✓
    "DEV_CODE",         # ✓
    "EFAC_DTEEDI",      # ✓ delta + remplace EFAC_DATE (x WARN)
    "EFAC_DTEECH",      # ✓ remplace EFAC_ECHEANCE (x WARN)
    "EFAC_TYPF",        # ✓ remplace EFAC_TYPE (x WARN) — F/A
    "EFAC_TAUXTVA",     # ✓
    "EFAC_ORDNUM",      # ✓
    # x EFAC_MONTANTHT/TTC — absent DDL (montants calculés via WTLFAC)
    # x PRH_BTS            — absent DDL dans WTEFAC
]

_WTFAC = [
    "FAC_NUM",          # ✓
    "EFAC_NUM",         # + extra DDL disponible
    "CALF_AN",          # +
    "CALF_NPERIODE",    # +
    "LOTFAC_CODE",      # +
    # x FAC_PERIODE       — absent DDL
    # x FAC_DATEMODIF     — absent DDL → FULL-LOAD
]

_WTLFAC = [
    "FAC_NUM",          # ✓
    "LFAC_ORD",         # ✓
    "LFAC_LIB",         # + remplace LFAC_LIBELLE (x absent DDL)
    "LFAC_BASE",        # ✓ bronze
    "LFAC_TAUX",        # ✓
    "LFAC_MNT",         # ✓
    # x LFAC_RUBRIQUE     — absent DDL
]

_CMFACTURES = [
    "FACT_ID",          # ✓
    "TIE_ID",           # ✓
    # x FACT_MONTANTHT/TTC/TVA — absent DDL (montants dans CMFACTDET)
    # x FACT_DATEMODIF   — absent DDL → FULL-LOAD
]

_WTFACINFO = [
    "CNT_ID",           # ✓
    "FAC_NUM",          # ✓
    "PER_ID",           # ✓
    "TIE_ID",           # ✓
    # x EFAC_NUM         — absent DDL
    # x RGPCNT_ID        — absent DDL
    # x FACINFO_DATEMODIF — absent DDL → FULL-LOAD
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : TEMPS & RELEVÉS D'HEURES
# ─────────────────────────────────────────────────────────────────

_WTPRH = [
    "PRH_BTS",          # ✓ clé
    "PER_ID",           # ✓
    "CNT_ID",           # ✓
    "TIE_ID",           # ✓
    "PRH_DTEDEBSEM",    # ✓
    "LOTPAYE_CODE",     # ✓
    "CAL_AN",           # ✓
    "CAL_NPERIODE",     # ✓
    "LOTFAC_CODE",      # ✓
    "CALF_AN",          # ✓
    "CALF_NPERIODE",    # ✓
    "CNTI_ORDRE",       # ✓
    "PRH_DTEFINSEM",    # ✓
    "VENTHEU_DTEDEB",   # ✓
    "PRH_IFM",          # ✓
    "PRH_CP",           # ✓
    "PRH_FLAG_RH",      # ✓ Bronze — proxy is_valide
    "PRH_MODIFDATE",    # ✓ delta col Bronze
    # x RGPCNT_ID        — WARN probe (absent DDL)
    # x PRH_PERIODE      — WARN probe
    # x PRH_VALIDE       — WARN probe (doublon PRH_FLAG_RH)
    # x PRH_DATEMODIF    — WARN probe (doublon PRH_MODIFDATE)
]

_WTRHDON = [
    "RINT_ID",          # ✓
    "RHD_LIGNE",        # ✓
    "PRH_BTS",          # ✓
    "FAC_NUM",          # ✓
    "BUL_ID",           # ✓
    "RHD_BASEP",        # ✓ Bronze — base paye
    "RHD_TAUXP",        # ✓ Bronze — taux paye
    "RHD_BASEF",        # ✓ Bronze — base fact
    "RHD_TAUXF",        # ✓ Bronze — taux fact
    "RHD_RAPPEL",       # ✓
    "RHD_ORIRUB",       # ✓ Bronze — code rubrique
    "RHD_PORTEE",       # ✓
    "RHD_LIBRUB",       # ✓ Bronze — libellé rubrique
    "RHD_EXCLDEP",      # ✓
    "RHD_SEUILP",       # ✓
    "RHD_SEUILF",       # ✓
    "RHD_BASEPROV",     # ✓
    "RHD_TAUXPROV",     # ✓
    "RHD_DATED",        # ✓ delta col
    "RHD_DATEF",        # ✓
    # x RHD_BASEPAYE/TAUXPAYE/BASEFACT/TAUXFACT — WARN (doublons RHD_BASEP/P/F)
    # x RHD_RUBRIQUE/LIBELLE — WARN (doublons RHD_ORIRUB/LIBRUB)
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : RÉFÉRENTIELS (Master Data)
# ─────────────────────────────────────────────────────────────────

_WTMET = [
    "MET_ID",           # ✓
    "MET_CODE",         # ✓ bronze (non-missing)
    "MET_LIBELLE",      # + remplace MET_LIB (x absent DDL)
    "TQUA_ID",          # + remplace MET_QUALIFICATION (x absent DDL)
    "SPE_ID",           # + remplace MET_SPECIALITE (x)
    "NIVQ_ID",          # + remplace MET_NIVEAU (x)
    "PCS_CODE_2003",    # + remplace MET_PCSCODE (x)
    # x MET_DATEMODIF    — absent DDL → FULL-LOAD
]

_WTQUA = [
    "TQUA_ID",          # + (x QUA_CODE absent DDL, remplacé)
    "TQUA_CODE",        # +
    "TQUA_LIBELLE",     # + remplace QUA_LIB (x)
    # x QUA_DATEMODIF    — absent DDL
]

_WTNAF2008 = [
    "NAF2008",          # + remplace NAF_CODE (x absent DDL)
    "NAF_LIB2008",      # + remplace NAF_LIB (x)
    "NAFANC",           # + code NAF ancien (extra)
]

_WTCOMMUNE = [
    "INSEE_CDE",        # + remplace COM_INSEE (x absent DDL)
    "COMMUNE_LIBELLE",  # + remplace COM_NOM (x)
    "CPO_CODE",         # + code postal
    "PAYS_CODE",        # +
    # x COM_DEP/COM_REGION — absent DDL
]

_WTTHAB = [
    "THAB_ID",          # ✓
    "THAB_CDE",         # + remplace THAB_CODE (x absent DDL)
    "THAB_LIBELLE",     # + remplace THAB_LIB (x)
    # x THAB_ORGANISME   — absent DDL (via WTORG)
]

_WTORG = [
    "ORG_NOM",          # + remplace ORG_LIB (x absent DDL)
    # ✓ (non-missing probe = ORG_ID x absent, mais ORG_CODE confirmé)
    "ORG_CODE",
    "ORG_NOM_COURT",    # +
    "ORG_CP",           # +
    # x ORG_ID           — absent DDL (remplacé par ORG_CODE)
]

_WTTDIP = [
    "TDIP_ID",          # ✓
    "TDIP_CODE",        # ✓ (non-missing)
    "TDIP_LIB",         # ✓
    "TDIP_REF",         # + remplace TDIP_DOMAINE (x absent DDL)
]

_WTFINMISS = [
    "FINMISS_CODE",     # ✓
    "FINMISS_LIBELLE",  # + remplace FINMISS_LIB (x absent DDL)
    "FINMISS_IFM",      # ~ (non-réfuté)
    "FINMISS_CP",       # ~
]

_WTNIVQ = [
    "NIVQ_ID",          # + (extra DDL)
    "NIVQ_CODE",        # ✓ (non-missing)
    "NIVQ_LIBELLE",     # + remplace NIVQ_LIB (x absent DDL)
]

_WTSPE = [
    "SPE_ID",           # + (extra DDL)
    "SPE_CODE",         # ✓
    "SPE_LIB",          # ✓
    "TQUA_ID",          # + (extra — FK qualification)
    # x SPE_MET_ID       — absent DDL
]

_WTUGAG = [
    "RGPCNT_ID",        # ✓
    "TIE_ID",           # ✓
    "UGAG_MT",          # + remplace UGAG_MONTANT (x absent DDL)
    # x UGAG_DATE/DATEMODIF — absent DDL → FULL-LOAD
]

_WTNIVQ_extra = [       # noms issus probe extra
    "NIVQ_ID",
    "NIVQ_LIBELLE",
    "SPE_ID",
]

_CMDEVISES = [
    "DEV_CODE",         # ✓
    "DEV_LIB",          # ~ (non-réfuté)
    "DEV_SIGLE",        # + (extra DDL)
    "DEV_NBDEC",        # +
    # x DEV_TAUXCHANGE   — absent DDL
]

# ─────────────────────────────────────────────────────────────────
# DOMAINE : INTÉRIMAIRES — tables complémentaires
# ─────────────────────────────────────────────────────────────────

_WTPSTG = [
    "PER_ID",           # ✓
    "PSTG_ORDRE",       # + remplace ORDRE (x absent DDL)
    "TSTG_ID",          # + (extra) type de stage
    "PSTG_DEBUT",       # + remplace PSTG_DATEDEB (x)
    "PSTG_FIN",         # + remplace PSTG_DATEFIN (x)
    # x PSTG_LIB/ORGANISME/DATEMODIF — absent DDL
]

_PYCONTRAT = [
    "PER_ID",           # ✓
    "CNT_ID",           # ✓
    "RGPCNT_ID",        # ~ (non-réfuté)
    "ETA_ID",           # ~
    # x CNTR_TYPE/DATEDEB/DATEFIN/ESSAI/LOTPAYE/DATEMODIF — absent DDL
    # Les colonnes PYCONTRAT sont préfixées CNT_* en réalité (voir extra)
]

_WTRFAN = [
    "TIE_ID",           # ✓
    "RFAN_ID",          # ~
    "RGPCNT_ID",        # + (extra DDL disponible)
    # x RFAN_AN/DATEMODIF — absent DDL
]

_WTCALF = [
    "LOTFAC_CODE",      # ✓
    "CALF_AN",          # ✓
    "CALF_NPERIODE",    # ✓
    "CALF_DEBUT",       # + remplace CALF_DATEDEB (x absent DDL)
    "CALF_FIN",         # + remplace CALF_DATEFIN (x)
]

# ─────────────────────────────────────────────────────────────────
# EXPORT PUBLIC
# ─────────────────────────────────────────────────────────────────

EXPECTED: dict[str, list[str]] = {
    # Intérimaires
    "PYPERSONNE": _PYPERSONNE,
    "PYSALARIE": _PYSALARIE,
    "WTPINT": _WTPINT,
    "PYCOORDONNEE": _PYCOORDONNEE,
    "WTPMET": _WTPMET,
    "WTPHAB": _WTPHAB,
    "WTPDIP": _WTPDIP,
    "WTEXP": _WTEXP,
    "WTPEVAL": _WTPEVAL,
    "RHPERSONNE": _RHPERSONNE,
    # Clients
    "WTTIESERV": _WTTIESERV,
    "WTCLPT": _WTCLPT,
    "WTTIEINT": _WTTIEINT,
    "WTCOEF": _WTCOEF,
    "WTENCOURSG": _WTENCOURSG,
    "WTUGCLI": _WTUGCLI,
    # Agences
    "WTUG": _WTUG,
    "PYETABLISSEMENT": _PYETABLISSEMENT,
    # Missions & Contrats
    "WTMISS": _WTMISS,
    "WTCNTI": _WTCNTI,
    "WTCMD": _WTCMD,
    "WTPLAC": _WTPLAC,
    # Facturation
    "WTEFAC": _WTEFAC,
    "WTFAC": _WTFAC,
    "WTLFAC": _WTLFAC,
    "CMFACTURES": _CMFACTURES,
    "WTFACINFO": _WTFACINFO,
    # Temps
    "WTPRH": _WTPRH,
    "WTRHDON": _WTRHDON,
    # Intérimaires complémentaires
    "WTPSTG": _WTPSTG,
    # Contrats paie
    "PYCONTRAT": _PYCONTRAT,
    # Facturation complémentaires
    "WTRFAN": _WTRFAN,
    "WTCALF": _WTCALF,
    # Agences complémentaires
    "WTUGAG": _WTUGAG,
    # Référentiels
    "WTMET": _WTMET,
    "WTQUA": _WTQUA,
    "WTNIVQ": _WTNIVQ,
    "WTSPE": _WTSPE,
    "WTTHAB": _WTTHAB,
    "WTORG": _WTORG,
    "WTTDIP": _WTTDIP,
    "WTFINMISS": _WTFINMISS,
    "WTNAF2008": _WTNAF2008,
    "WTCOMMUNE": _WTCOMMUNE,
    "CMDEVISES": _CMDEVISES,
}

# Colonnes dont le statut DDL reste incertain (non confirmées, non réfutées)
UNCERTAIN_COLUMNS: dict[str, list[str]] = {
    # présents en _COLS mais non testés probe
    "WTPRH": ["PRH_DTEDEBSEM", "LOTPAYE_CODE", "CNTI_ORDRE"],
    "PYCONTRAT": ["RGPCNT_ID", "ETA_ID"],
}
