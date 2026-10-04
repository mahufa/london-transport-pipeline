from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).parents[1] / 'dags'
EXPECTED_DAG_IDS = {'tfl_bikes', 'tfl_chargers', 'tfl_roads', 'loader', 'transformer'}


@pytest.fixture(scope='session')
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_are_loaded(dag_bag):
    assert EXPECTED_DAG_IDS <= set(dag_bag.dag_ids)
