-- ddl_gold_finitions.sql — Phase 7 #21 — GI Data Lakehouse · PostgreSQL Gold
-- Index manquants + COMMENT ON TABLE/COLUMN.
-- Idempotent : CREATE INDEX IF NOT EXISTS / COMMENT ON est idempotent par nature.
-- Exécuter avec avnadmin (COMMENT ON nécessite ownership ou superuser) :
--   psql "postgresql://avnadmin:<password>@<host>:20184/gi_data?sslmode=require" \
--        -f ddl_gold_finitions.sql

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Index manquants
-- ─────────────────────────────────────────────────────────────────────────────

-- fact_ca_mensuel_client : filtres temporels (Superset date range) + drill agence
CREATE INDEX IF NOT EXISTS fact_ca_mensuel_client_mois_idx
    ON gld_commercial.fact_ca_mensuel_client (mois);
CREATE INDEX IF NOT EXISTS fact_ca_mensuel_client_agence_idx
    ON gld_commercial.fact_ca_mensuel_client (agence_principale);

-- fact_retention_client : filtres temporels par trimestre
CREATE INDEX IF NOT EXISTS fact_retention_client_trimestre_idx
    ON gld_clients.fact_retention_client (trimestre);

-- fact_rupture_contrat : dashboard agence (agence_id, mois) — PK (agence_id, tie_id, mois) ne couvre pas ce filtre directement
CREATE INDEX IF NOT EXISTS fact_rupture_contrat_agence_mois_idx
    ON gld_performance.fact_rupture_contrat (agence_id, mois);

-- fact_heures_hebdo : lookup par client (tie_id) depuis vue client
CREATE INDEX IF NOT EXISTS fact_heures_hebdo_tie_id_idx
    ON gld_operationnel.fact_heures_hebdo (tie_id);

-- fact_competences_dispo : lookup par agence (rgpcnt_id) — PK est (met_id, rgpcnt_id)
CREATE INDEX IF NOT EXISTS fact_competences_dispo_rgpcnt_idx
    ON gld_staffing.fact_competences_dispo (rgpcnt_id);

-- fact_activite_int : filtres temporels par mois
CREATE INDEX IF NOT EXISTS fact_activite_int_mois_idx
    ON gld_staffing.fact_activite_int (mois);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. COMMENT ON TABLE
-- ─────────────────────────────────────────────────────────────────────────────

-- gld_shared
COMMENT ON TABLE gld_shared.dim_calendrier IS
    'Dimension temps — une ligne par date calendaire. is_jour_ouvre exclut week-ends et jours fériés français.';
COMMENT ON TABLE gld_shared.dim_agences IS
    'Dimension agences — une ligne par agence active Evolia (WTRGPCNT). Clé naturelle : rgpcnt_id.';
COMMENT ON TABLE gld_shared.dim_clients IS
    'Dimension clients — une ligne par client actuel (is_current=true dédupliqué). Clé naturelle : tie_id (WTTIERS).';
COMMENT ON TABLE gld_shared.dim_interimaires IS
    'Dimension intérimaires — une ligne par intérimaire actuel (is_current=true dédupliqué). Clé naturelle : per_id (WTPERSONNE).';
COMMENT ON TABLE gld_shared.dim_metiers IS
    'Dimension métiers — qualifications/spécialités issues de WTMETIER. Clé naturelle : met_id.';
COMMENT ON TABLE gld_shared.dim_habilitations IS
    'Référentiel habilitations — codes WTTHAB (type=HABILITATION). Alimenté par silver_competences.py.';
COMMENT ON TABLE gld_shared.dim_diplomes IS
    'Référentiel diplômes — codes WTTDIP (type=DIPLOME). Alimenté par silver_competences.py.';
COMMENT ON TABLE gld_shared.ref_naf_sections IS
    'Référentiel INSEE sections NAF A-U. Sert à ventiler le CA par grande famille sectorielle dans fact_ca_secteur_naf.';

-- gld_commercial
COMMENT ON TABLE gld_commercial.fact_ca_mensuel_client IS
    'CA mensuel par client (tie_id, mois). Source : slv_facturation/entetes_factures + lignes_factures. ca_net_ht = ca_ht - avoir_ht.';
COMMENT ON TABLE gld_clients.fact_concentration_client IS
    'Concentration CA par agence/mois : top-20 clients vs total. taux_concentration = ca_net_top20 / ca_net_total.';
COMMENT ON TABLE gld_commercial.fact_renouvellement_mission IS
    'Taux de reconduction mission chez même client par agence/mois. taux_renouvellement_pct = nb_renouvellements / nb_missions * 100.';
COMMENT ON TABLE gld_commercial.fact_ca_secteur_naf IS
    'Ventilation CA par section NAF INSEE (A-U) par agence/mois. Jointure sur naf_code depuis dim_clients.';

-- gld_clients
COMMENT ON TABLE gld_clients.vue_360_client IS
    'Vue client enrichie — snapshot courant par tie_id. Agrège CA, missions actives, risque churn, DSO, compétences, encours.';
COMMENT ON TABLE gld_clients.fact_retention_client IS
    'Rétention client par trimestre — deltas CA QoQ/YoY, fréquence d''activité sur 4 trimestres glissants, score churn.';
COMMENT ON TABLE gld_clients.fact_rentabilite_client IS
    'Rentabilité annuelle client — marge brute (CA missions - coût payé), taux marge, coût gestion estimé.';

-- gld_performance
COMMENT ON TABLE gld_performance.fact_rupture_contrat IS
    'Qualité missions : ruptures et fins anticipées par agence/client/mois. taux_rupture_pct = nb_ruptures / nb_missions_total * 100.';
COMMENT ON TABLE gld_performance.scorecard_agence IS
    'Scorecard mensuelle agence — CA, marge, nb clients actifs, nb intérimaires, taux transformation commandes.';
COMMENT ON TABLE gld_performance.ranking_agences IS
    'Classement mensuel des agences sur 4 axes (CA, marge, placement, transformation). score_global = moyenne pondérée des rangs.';
COMMENT ON TABLE gld_performance.tendances_agence IS
    'Tendances mensuelles agence — deltas MoM et YoY sur CA, marge, intérimaires. tendance : UP/DOWN/STABLE.';
COMMENT ON TABLE gld_performance.fact_duree_mission IS
    'Durée moyenne mission (DMM) par agence/client/métier/mois. profil_duree : MICRO(<1j) / COURTE(<1sem) / MOYENNE(<1mois) / LONGUE.';
COMMENT ON TABLE gld_performance.fact_coeff_facturation IS
    'Coefficient facturation (THF/THP) pondéré par heures par agence/client/métier/mois. niveau_coeff : BON / MOYEN / A_REVOIR.';

-- gld_operationnel
COMMENT ON TABLE gld_operationnel.fact_etp_hebdo IS
    'Équivalent Temps Plein hebdomadaire par agence. etp = heures_totales / 35 (semaine légale). Source : slv_temps/etp_hebdo.';
COMMENT ON TABLE gld_operationnel.fact_heures_hebdo IS
    'Heures payées vs facturées par agence/client/semaine. Écart heures_fact - heures_paye = surcoût ou perte.';
COMMENT ON TABLE gld_operationnel.fact_commandes_pipeline IS
    'Commandes pipeline par agence/semaine. taux_satisfaction = nb_pourvues / nb_commandes.';
COMMENT ON TABLE gld_operationnel.fact_delai_placement IS
    'Délai de placement mission par agence/semaine. pct_moins_4h/pct_meme_jour/pct_plus_3j = distribution des délais.';
COMMENT ON TABLE gld_operationnel.fact_echus_hebdo IS
    'Encours hebdomadaire échu par client/agence. montant_echu_ht = factures non réglées dont date_echeance dépassée.';
COMMENT ON TABLE gld_operationnel.fact_conformite_dpae IS
    'Conformité DPAE (Déclaration Préalable À l''Embauche) par agence/mois. taux = nb_dpae_conformes / nb_missions.';
COMMENT ON TABLE gld_operationnel.fact_dso_client IS
    'DSO (Days Sales Outstanding) par agence/client/mois. dso_jours = encours_ht / (ca_mensuel / nb_jours_mois).';
COMMENT ON TABLE gld_operationnel.fact_balance_agee IS
    'Balance âgée par agence/mois/tranche (0-30j, 31-60j, 61-90j, >90j). Source : fact_dso_client agrégé par tranche retard.';

-- gld_staffing
COMMENT ON TABLE gld_staffing.fact_competences_dispo IS
    'Disponibilité compétences par métier/agence. taux_couverture = nb_disponibles / NULLIF(nb_qualifies, 0).';
COMMENT ON TABLE gld_staffing.fact_activite_int IS
    'Activité mensuelle intérimaire — missions, agences, clients, heures, CA généré. taux_occupation = heures_travaillees / heures_disponibles.';
COMMENT ON TABLE gld_staffing.fact_missions_detail IS
    'Détail mission par intérimaire/contrat/client. Grain : (per_id, cnt_id, tie_id) dédupliqué SCD2. marge_mission = ca_mission - cout_mission.';
COMMENT ON TABLE gld_staffing.fact_fidelisation_interimaires IS
    'Fidélisation intérimaires par agence/catégorie. categorie_fidelisation : FIDELE / OCCASIONNEL / NOUVEAU / INACTIF.';
COMMENT ON TABLE gld_staffing.fact_dynamique_vivier IS
    'Dynamique du vivier intérimaires par agence/mois — entrées, sorties, croissance nette, taux renouvellement.';

-- ops
COMMENT ON TABLE ops.pipeline_watermarks IS
    'Watermarks de pipeline — dernière exécution réussie par (pipeline, table). Mis à jour par shared.py après chaque run Gold.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. COMMENT ON COLUMN — clés de substitution + champs non évidents
-- ─────────────────────────────────────────────────────────────────────────────

-- ── gld_shared.dim_agences ──────────────────────────────────────────────────
COMMENT ON COLUMN gld_shared.dim_agences.agence_sk IS
    'Clé de substitution — MD5(rgpcnt_id::text). Utilisée comme FK dans toutes les tables Gold.';
COMMENT ON COLUMN gld_shared.dim_agences.rgpcnt_id IS
    'Clé naturelle agence — WTRGPCNT.RGPCNT_ID dans Evolia/SQL Server.';

-- ── gld_shared.dim_clients ──────────────────────────────────────────────────
COMMENT ON COLUMN gld_shared.dim_clients.client_sk IS
    'Clé de substitution — MD5(tie_id::text). Utilisée comme FK dans toutes les tables Gold.';
COMMENT ON COLUMN gld_shared.dim_clients.tie_id IS
    'Clé naturelle client — WTTIERS.TIE_ID dans Evolia/SQL Server.';
COMMENT ON COLUMN gld_shared.dim_clients.naf_libelle IS
    'Libellé NAF issu de Salesforce (Code_NAF__r.name) à 68% de couverture. NULL = client Evolia sans correspondance SF.';
COMMENT ON COLUMN gld_shared.dim_clients.effectif_tranche IS
    'Tranche effectif SIRENE (texte libre, ex. "50 à 99 salariés"). Non normalisé — usage affichage uniquement.';
COMMENT ON COLUMN gld_shared.dim_clients.statut_client IS
    'Statut commercial dans Evolia : ACTIF / PROSPECT / INACTIF / SUSPENDU.';

-- ── gld_shared.dim_interimaires ─────────────────────────────────────────────
COMMENT ON COLUMN gld_shared.dim_interimaires.interimaire_sk IS
    'Clé de substitution — MD5(per_id::text). Utilisée comme FK dans toutes les tables Gold.';
COMMENT ON COLUMN gld_shared.dim_interimaires.per_id IS
    'Clé naturelle intérimaire — WTPERSONNE.PER_ID dans Evolia/SQL Server.';
COMMENT ON COLUMN gld_shared.dim_interimaires.agence_rattachement IS
    'rgpcnt_id de l''agence principale de l''intérimaire (WTUGPINT.RGPCNT_ID, affectation la plus récente). NULL si inconnu.';
COMMENT ON COLUMN gld_shared.dim_interimaires.is_actif IS
    'True si l''intérimaire a eu au moins une mission dans les 12 derniers mois.';
COMMENT ON COLUMN gld_shared.dim_interimaires.is_candidat IS
    'True si le dossier intérimaire est en phase de candidature (pas encore placé).';

-- ── gld_shared.dim_metiers ──────────────────────────────────────────────────
COMMENT ON COLUMN gld_shared.dim_metiers.metier_sk IS
    'Clé de substitution — MD5(met_id::text).';
COMMENT ON COLUMN gld_shared.dim_metiers.met_id IS
    'Clé naturelle métier — WTMETIER.MET_ID dans Evolia/SQL Server.';
COMMENT ON COLUMN gld_shared.dim_metiers.pcs_code IS
    'Code PCS-ESE INSEE (Professions et Catégories Socioprofessionnelles). NULL si non renseigné en source.';

-- ── gld_shared.dim_calendrier ───────────────────────────────────────────────
COMMENT ON COLUMN gld_shared.dim_calendrier.jour_semaine IS
    'Entier 0=lundi … 6=dimanche (convention DuckDB ISODOW-1). Différent de la convention SQL standard (0=dimanche).';
COMMENT ON COLUMN gld_shared.dim_calendrier.semaine_iso IS
    'Numéro de semaine ISO 8601 (1-53). La semaine 1 est celle contenant le premier jeudi de l''année.';

-- ── gld_commercial.fact_ca_mensuel_client ────────────────────────────────────
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.client_sk IS
    'MD5(tie_id::text) — FK vers gld_shared.dim_clients. NULL possible si client hors périmètre dim.';
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.mois IS
    'Premier jour du mois (DATE tronquée au 1er). Ex : 2026-03-01 = mars 2026.';
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.ca_net_ht IS
    'CA net HT = ca_ht - avoir_ht. Valeur de référence pour la pyramide de validation.';
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.nb_heures_facturees IS
    'Heures facturées issues de WTLFAC.LFAC_BASE (quantité). NULL si aucune ligne facture correspondante.';
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.taux_moyen_fact IS
    'Taux horaire moyen de facturation = ca_ht / nb_heures_facturees. NULL si nb_heures = 0.';
COMMENT ON COLUMN gld_commercial.fact_ca_mensuel_client.agence_principale IS
    'rgpcnt_id de l''agence ayant généré le plus de CA pour ce client ce mois-ci.';

-- ── gld_clients.vue_360_client ──────────────────────────────────────────────
COMMENT ON COLUMN gld_clients.vue_360_client.ca_ytd IS
    'CA net HT depuis le 1er janvier de l''année courante.';
COMMENT ON COLUMN gld_clients.vue_360_client.ca_n1 IS
    'CA net HT sur l''année civile précédente (N-1).';
COMMENT ON COLUMN gld_clients.vue_360_client.delta_ca_pct IS
    'Variation CA YTD vs YTD N-1 en %. NULL si CA N-1 = 0.';
COMMENT ON COLUMN gld_clients.vue_360_client.top_3_metiers IS
    'JSON array des 3 métiers les plus fréquents pour ce client (libelle_metier). Ex : ["Cariste","Soudeur","Manutentionnaire"].';
COMMENT ON COLUMN gld_clients.vue_360_client.risque_credit_score IS
    'Évaluation risque crédit : LOW / MEDIUM / HIGH — basée sur encours/limite et historique retards.';
COMMENT ON COLUMN gld_clients.vue_360_client.risque_churn IS
    'Probabilité de départ client : LOW / MEDIUM / HIGH — basée sur jours_depuis_derniere_facture et tendance CA.';
COMMENT ON COLUMN gld_clients.vue_360_client.jours_depuis_derniere IS
    'Nombre de jours depuis la dernière facture émise. 9999 si aucune facture connue.';

-- ── gld_clients.fact_retention_client ───────────────────────────────────────
COMMENT ON COLUMN gld_clients.fact_retention_client.trimestre IS
    'Premier jour du trimestre (DATE tronquée). Ex : 2026-01-01 = Q1 2026.';
COMMENT ON COLUMN gld_clients.fact_retention_client.frequence_4_trimestres IS
    'Nombre de trimestres actifs (CA > 0) sur les 4 derniers trimestres glissants. 4 = client très fidèle.';
COMMENT ON COLUMN gld_clients.fact_retention_client.risque_churn IS
    'LOW / MEDIUM / HIGH — calculé sur fréquence_4_trimestres + jours_inactivite.';
COMMENT ON COLUMN gld_clients.fact_retention_client.churn_score_ml IS
    'Score ML churn [0-1]. Réservé Phase 2 — NULL en production actuelle.';

-- ── gld_clients.fact_rentabilite_client ─────────────────────────────────────
COMMENT ON COLUMN gld_clients.fact_rentabilite_client.marge_brute IS
    'Marge brute = ca_missions - cout_paye. Peut être négative (mission déficitaire).';
COMMENT ON COLUMN gld_clients.fact_rentabilite_client.cout_gestion_estime IS
    'Coût de gestion administrative estimé (% appliqué au CA). Paramètre configurable dans gold_retention_client.py.';
COMMENT ON COLUMN gld_clients.fact_rentabilite_client.rentabilite_nette IS
    'Rentabilité nette = marge_brute - cout_gestion_estime.';

-- ── gld_performance.fact_rupture_contrat ────────────────────────────────────
COMMENT ON COLUMN gld_performance.fact_rupture_contrat.nb_ruptures IS
    'Nombre de missions terminées avant la date de fin prévue (status rupture dans WTCNT).';
COMMENT ON COLUMN gld_performance.fact_rupture_contrat.nb_annulations IS
    'Nombre de missions annulées avant démarrage.';
COMMENT ON COLUMN gld_performance.fact_rupture_contrat.nb_terme_normal IS
    'Nombre de missions allées à leur terme prévu.';
COMMENT ON COLUMN gld_performance.fact_rupture_contrat.duree_moy_avant_rupture IS
    'Durée moyenne en jours entre début de mission et la rupture (uniquement sur missions rompues).';

-- ── gld_performance.scorecard_agence ────────────────────────────────────────
COMMENT ON COLUMN gld_performance.scorecard_agence.taux_transformation IS
    'Taux de transformation commandes = nb_pourvues / NULLIF(nb_commandes, 0).';
COMMENT ON COLUMN gld_performance.scorecard_agence.taux_marge IS
    'Taux de marge brute = marge_brute / NULLIF(ca_net_ht, 0).';

-- ── gld_performance.ranking_agences ─────────────────────────────────────────
COMMENT ON COLUMN gld_performance.ranking_agences.rang_ca IS
    'Rang agence sur le CA net (1 = meilleure agence ce mois). Calculé RANK() OVER (ORDER BY ca_net_ht DESC).';
COMMENT ON COLUMN gld_performance.ranking_agences.score_global IS
    'Score composite = moyenne arithmétique des 4 rangs normalisés [0-1]. Plus proche de 0 = meilleur.';

-- ── gld_performance.tendances_agence ────────────────────────────────────────
COMMENT ON COLUMN gld_performance.tendances_agence.tendance IS
    'Tendance synthétique agence : UP (CA MoM > +5%) / DOWN (CA MoM < -5%) / STABLE.';

-- ── gld_performance.fact_duree_mission ──────────────────────────────────────
COMMENT ON COLUMN gld_performance.fact_duree_mission.profil_duree IS
    'Profil dominant : MICRO (<1j) / COURTE (<1 semaine) / MOYENNE (<1 mois) / LONGUE (≥1 mois).';
COMMENT ON COLUMN gld_performance.fact_duree_mission.dmm_semaines IS
    'Durée Moyenne Mission en semaines = dmm_jours / 5 (jours ouvrés).';

-- ── gld_performance.fact_coeff_facturation ──────────────────────────────────
COMMENT ON COLUMN gld_performance.fact_coeff_facturation.coeff_moyen_pondere IS
    'Coefficient THF/THP pondéré par heures = SUM(THF*h) / SUM(THP*h). Reflète le vrai mix de facturation.';
COMMENT ON COLUMN gld_performance.fact_coeff_facturation.niveau_coeff IS
    'Classification : BON (coeff ≥ seuil cible) / MOYEN / A_REVOIR (coeff < seuil alerte). Seuils configurables.';
COMMENT ON COLUMN gld_performance.fact_coeff_facturation.marge_coeff_pct IS
    'Marge implicite du coefficient = (coeff_moyen_pondere - 1) * 100. Ex : 45.2 = 45.2% de marge brute structurelle.';

-- ── gld_operationnel.fact_delai_placement ───────────────────────────────────
COMMENT ON COLUMN gld_operationnel.fact_delai_placement.pct_moins_4h IS
    '% de missions placées en moins de 4 heures (réactivité excellente).';
COMMENT ON COLUMN gld_operationnel.fact_delai_placement.pct_meme_jour IS
    '% de missions placées dans la même journée (≤ 24h après la commande).';
COMMENT ON COLUMN gld_operationnel.fact_delai_placement.pct_plus_3j IS
    '% de missions placées en plus de 72h (alerte opérationnelle, risque perte commande).';

-- ── gld_operationnel.fact_conformite_dpae ───────────────────────────────────
COMMENT ON COLUMN gld_operationnel.fact_conformite_dpae.taux_conformite_dpae IS
    'Taux DPAE conformes = nb_dpae_conformes / NULLIF(nb_missions, 0). nb_dpae_conformes = DPAE transmises avec ecart_heures ≤ 0.';
COMMENT ON COLUMN gld_operationnel.fact_conformite_dpae.nb_dpae_conformes IS
    'DPAE transmises avant ou à l''heure d''embauche (ecart_heures ≤ 0).';
COMMENT ON COLUMN gld_operationnel.fact_conformite_dpae.nb_dpae_retard IS
    'DPAE transmises en retard (ecart_heures > 0) — risque de verbalisation URSSAF.';
COMMENT ON COLUMN gld_operationnel.fact_conformite_dpae.ecart_moyen_heures IS
    'Écart moyen en heures entre l''heure d''embauche et la transmission DPAE. Positif = retard.';

-- ── gld_operationnel.fact_dso_client ────────────────────────────────────────
COMMENT ON COLUMN gld_operationnel.fact_dso_client.encours_ht IS
    'Encours factures ouvertes HT à la date de snapshot (non réglées).';
COMMENT ON COLUMN gld_operationnel.fact_dso_client.dso_jours IS
    'DSO = encours_ht / (ca_mensuel / nb_jours_mois). NULL si ca_mensuel = 0.';
COMMENT ON COLUMN gld_operationnel.fact_dso_client.montant_echu IS
    'Montant HT des factures dont la date d''échéance est dépassée au moment du snapshot.';

-- ── gld_operationnel.fact_balance_agee ──────────────────────────────────────
COMMENT ON COLUMN gld_operationnel.fact_balance_agee.tranche IS
    'Tranche de retard : ''0-30j'' / ''31-60j'' / ''61-90j'' / ''>90j''. Calculé sur date_echeance vs snapshot courant.';

-- ── gld_staffing.fact_missions_detail ───────────────────────────────────────
COMMENT ON COLUMN gld_staffing.fact_missions_detail.mission_sk IS
    'Clé de substitution — MD5(per_id || ''|'' || cnt_id || ''|'' || tie_id). Unique par couple intérimaire/contrat/client.';
COMMENT ON COLUMN gld_staffing.fact_missions_detail.marge_horaire IS
    'Marge par heure = taux_horaire_fact - taux_horaire_paye. En euros/heure.';
COMMENT ON COLUMN gld_staffing.fact_missions_detail.taux_marge IS
    'Taux de marge mission = marge_mission / NULLIF(ca_mission, 0).';

-- ── gld_staffing.fact_fidelisation_interimaires ─────────────────────────────
COMMENT ON COLUMN gld_staffing.fact_fidelisation_interimaires.categorie_fidelisation IS
    'Catégorie : FIDELE (>= 3 missions/12 mois) / OCCASIONNEL (1-2) / NOUVEAU (<= 3 mois ancienneté) / INACTIF.';
COMMENT ON COLUMN gld_staffing.fact_fidelisation_interimaires.taux_fidelisation_pct IS
    'Taux de fidélisation moyen = AVG(nb_missions_12m / (anciennete_jours/30)) par catégorie/agence. Phase 4 — peut être NULL.';

-- ── gld_staffing.fact_competences_dispo ─────────────────────────────────────
COMMENT ON COLUMN gld_staffing.fact_competences_dispo.nb_qualifies IS
    'Nombre d''intérimaires ayant cette compétence rattachés à cette agence.';
COMMENT ON COLUMN gld_staffing.fact_competences_dispo.nb_disponibles IS
    'Parmi les qualifiés, ceux sans mission active (disponibles pour placement).';
COMMENT ON COLUMN gld_staffing.fact_competences_dispo.nb_en_mission IS
    'Parmi les qualifiés, ceux actuellement en mission (indisponibles).';
COMMENT ON COLUMN gld_staffing.fact_competences_dispo.taux_couverture IS
    'Taux de couverture = nb_disponibles / NULLIF(nb_qualifies, 0). 0 = tout le vivier est en mission.';

-- ── gld_staffing.fact_dynamique_vivier ──────────────────────────────────────
COMMENT ON COLUMN gld_staffing.fact_dynamique_vivier.pool_actif IS
    'Snapshot ACTIF_RECENT à la fin du mois : intérimaires avec mission dans les 90 derniers jours.';
COMMENT ON COLUMN gld_staffing.fact_dynamique_vivier.taux_renouvellement_vivier_pct IS
    'Renouvellement = (nb_nouveaux + nb_perdus) / NULLIF(pool_total, 0) * 100. Mesure la volatilité du vivier.';

-- ── ops.pipeline_watermarks ─────────────────────────────────────────────────
COMMENT ON COLUMN ops.pipeline_watermarks.last_success IS
    'Horodatage de la dernière exécution réussie (rows_loaded > 0). NULL si jamais exécuté avec succès.';
COMMENT ON COLUMN ops.pipeline_watermarks.rows_loaded IS
    'Nombre de lignes insérées lors du dernier run réussi. Sert de seuil d''alerte monitoring.';
