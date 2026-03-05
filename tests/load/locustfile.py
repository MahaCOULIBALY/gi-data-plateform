"""locustfile.py — Load testing Superset dashboards GI Data Lakehouse.
Simule 50 utilisateurs simultanés sur les 3 dashboards principaux.
Seuil : toutes requêtes < 3s (p95).

Usage:
    locust -f tests/load/locustfile.py --host https://gi-poc-platform.dataplatform.ovh.net
    # Puis ouvrir http://localhost:8089 → 50 users, spawn rate 5/s

Usage headless (CI) :
    locust -f tests/load/locustfile.py --host $SUPERSET_URL \
           --headless -u 50 -r 5 -t 5m \
           --csv tests/load/results \
           --exit-code-on-error 1
"""
import os
import json
import logging
from dataclasses import dataclass
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner

logger = logging.getLogger("gi-load-test")

# ── Configuration ──────────────────────────────────────────────
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

# Dashboard IDs — à adapter après création dans Superset
DASHBOARD_IDS = {
    "ca_agences": int(os.environ.get("DASH_CA_AGENCES_ID", "1")),
    "360_client": int(os.environ.get("DASH_360_CLIENT_ID", "2")),
    "perf_agences": int(os.environ.get("DASH_PERF_AGENCES_ID", "3")),
    "360_interimaire": int(os.environ.get("DASH_360_INT_ID", "4")),
}

# Seuil p95 en ms
SLA_P95_MS = 3000


@dataclass
class SupersetEndpoints:
    """Endpoints Superset à tester."""
    login: str = "/api/v1/security/login"
    csrf: str = "/api/v1/security/csrf_token/"
    dashboards: str = "/api/v1/dashboard/"
    chart_data: str = "/api/v1/chart/data"
    datasets: str = "/api/v1/dataset/"


EP = SupersetEndpoints()


class SupersetUser(HttpUser):
    """Simule un utilisateur Superset consultant les dashboards GI."""

    wait_time = between(2, 8)  # Pause réaliste entre actions
    access_token: str = ""
    csrf_token: str = ""

    def on_start(self) -> None:
        """Login Superset et récupération tokens."""
        resp = self.client.post(
            EP.login,
            json={
                "username": SUPERSET_USER,
                "password": SUPERSET_PASSWORD,
                "provider": "db",
            },
            name="POST /login",
        )
        if resp.status_code == 200:
            self.access_token = resp.json().get("access_token", "")
            # CSRF token
            csrf_resp = self.client.get(
                EP.csrf,
                headers={"Authorization": f"Bearer {self.access_token}"},
                name="GET /csrf",
            )
            if csrf_resp.status_code == 200:
                self.csrf_token = csrf_resp.json().get("result", "")
        else:
            logger.warning(f"Login failed: {resp.status_code}")

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token,
            "Content-Type": "application/json",
        }

    # ── Dashboard loads ────────────────────────────────────────

    @task(3)
    def load_dashboard_ca_agences(self) -> None:
        """Dashboard CA Agences — le plus consulté."""
        dash_id = DASHBOARD_IDS["ca_agences"]
        self.client.get(
            f"{EP.dashboards}{dash_id}",
            headers=self._headers(),
            name="GET /dashboard/ca_agences",
        )

    @task(2)
    def load_dashboard_360_client(self) -> None:
        """Dashboard 360° Client."""
        dash_id = DASHBOARD_IDS["360_client"]
        self.client.get(
            f"{EP.dashboards}{dash_id}",
            headers=self._headers(),
            name="GET /dashboard/360_client",
        )

    @task(2)
    def load_dashboard_perf_agences(self) -> None:
        """Dashboard Performance Agences."""
        dash_id = DASHBOARD_IDS["perf_agences"]
        self.client.get(
            f"{EP.dashboards}{dash_id}",
            headers=self._headers(),
            name="GET /dashboard/perf_agences",
        )

    @task(1)
    def load_dashboard_360_interimaire(self) -> None:
        """Dashboard 360° Intérimaire — accès restreint."""
        dash_id = DASHBOARD_IDS["360_interimaire"]
        self.client.get(
            f"{EP.dashboards}{dash_id}",
            headers=self._headers(),
            name="GET /dashboard/360_interimaire",
        )

    # ── Chart data queries ─────────────────────────────────────

    @task(3)
    def query_ca_hebdo(self) -> None:
        """Requête chart CA hebdo — simule chargement d'un chart Superset."""
        payload = {
            "datasource": {"id": 1, "type": "table"},
            "queries": [{
                "columns": ["agence_id", "semaine_iso"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(ca_net_ht)", "label": "ca_net"}],
                "orderby": [["ca_net", False]],
                "row_limit": 1000,
                "time_range": "Last 12 weeks",
            }],
        }
        self.client.post(
            EP.chart_data,
            json=payload,
            headers=self._headers(),
            name="POST /chart/ca_hebdo",
        )

    @task(2)
    def query_scorecard(self) -> None:
        """Requête scorecard agence — table avec ranking."""
        payload = {
            "datasource": {"id": 2, "type": "table"},
            "queries": [{
                "columns": ["agence_id", "mois", "ca_net_ht", "taux_marge", "score_global"],
                "orderby": [["score_global", False]],
                "row_limit": 500,
                "time_range": "Last month",
            }],
        }
        self.client.post(
            EP.chart_data,
            json=payload,
            headers=self._headers(),
            name="POST /chart/scorecard",
        )

    @task(2)
    def query_vue360_client(self) -> None:
        """Requête vue 360 client — table dénormalisée."""
        payload = {
            "datasource": {"id": 3, "type": "table"},
            "queries": [{
                "columns": ["raison_sociale", "ca_ytd", "nb_missions_actives", "risque_churn"],
                "filters": [{"col": "risque_churn", "op": "!=", "val": "LOW"}],
                "orderby": [["ca_ytd", False]],
                "row_limit": 200,
            }],
        }
        self.client.post(
            EP.chart_data,
            json=payload,
            headers=self._headers(),
            name="POST /chart/vue360_client",
        )

    @task(1)
    def list_datasets(self) -> None:
        """Vérification catalogue datasets."""
        self.client.get(
            EP.datasets,
            headers=self._headers(),
            name="GET /datasets",
        )


# ── SLA Validation (post-test) ────────────────────────────────

@events.quitting.add_listener
def check_sla(environment, **kwargs) -> None:
    """Vérifie que p95 < 3s pour toutes les requêtes."""
    stats = environment.runner.stats
    failures = []

    for entry in stats.entries.values():
        p95 = entry.get_response_time_percentile(0.95) or 0
        if p95 > SLA_P95_MS:
            failures.append(f"❌ {entry.name}: p95={p95:.0f}ms > {SLA_P95_MS}ms")

    if failures:
        logger.error("SLA VIOLATIONS:")
        for f in failures:
            logger.error(f)
        environment.process_exit_code = 1
    else:
        logger.info(f"✅ ALL SLA PASS: p95 < {SLA_P95_MS}ms for all endpoints")

    # Export résumé JSON
    summary = {
        "total_requests": stats.total.num_requests,
        "total_failures": stats.total.num_failures,
        "avg_response_ms": round(stats.total.avg_response_time, 1),
        "p95_response_ms": round(stats.total.get_response_time_percentile(0.95) or 0, 1),
        "p99_response_ms": round(stats.total.get_response_time_percentile(0.99) or 0, 1),
        "rps": round(stats.total.current_rps, 2),
        "sla_pass": len(failures) == 0,
        "violations": failures,
    }
    logger.info(json.dumps(summary, indent=2))
