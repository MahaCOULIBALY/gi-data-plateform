"""ge_suite_phase2.py — Great Expectations suites pour Phase 2.
Couvre : dim_interimaires, competences, fact_activite_int, fact_missions_detail.
"""
import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

SUITES = {
    "silver.dim_interimaires": [
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "per_id"}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_id", "mostly": 1.0}},
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "interimaire_sk"}},
        {"expectation_type": "expect_column_value_lengths_to_equal",
         "kwargs": {"column": "nir_pseudo", "value": 16, "mostly": 0.0}},  # OR null
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "date_naissance", "min_value": "1940-01-01", "max_value": "2010-01-01", "mostly": 0.95}},
    ],
    "silver.competences": [
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_id", "mostly": 1.0}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "type_competence", "value_set": ["METIER", "HABILITATION", "DIPLOME", "EXPERIENCE"]}},
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "competence_id"}},
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1000}},
    ],
    "gold.fact_activite_int": [
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_id", "mostly": 1.0}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "taux_occupation", "min_value": 0, "max_value": 2.0, "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "ca_genere", "min_value": 0, "mostly": 0.95}},
    ],
    "gold.fact_missions_detail": [
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "taux_marge", "min_value": -0.5, "max_value": 0.8, "mostly": 0.95}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "duree_jours", "min_value": 0, "mostly": 0.98}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "mission_sk", "mostly": 1.0}},
    ],
}


def create_suites():
    context = ge.get_context()
    for suite_name, expectations in SUITES.items():
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
        for exp in expectations:
            suite.add_expectation(ExpectationConfiguration(**exp))
        context.save_expectation_suite(suite)
        print(f"Created suite: {suite_name} ({len(expectations)} expectations)")


if __name__ == "__main__":
    create_suites()
