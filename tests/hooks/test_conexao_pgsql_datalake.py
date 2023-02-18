import logging
from uuid import uuid4
from airflow import DAG

from debussy_airflow.hooks.db_api_hook import (
    PostgreSQLConnectorHook,
    DbApiHookInterface
)
from typing import Callable, Iterable, Type
import datetime as dt

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator


def test_dag(dag_id):
    """
    simple dag definition for testing.
    for more complex definition instantiate your dag
    """
    default_args = {
        "owner": "debussy_framework_test",
        "retires": 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="Test dag",
        schedule_interval="0 5 * * *",
        catchup=False,
        start_date=dt.datetime(2022, 1, 1),
        max_active_runs=1,
        tags=["debussy_framework", "test dag"],
    )
    return dag


class TestHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        super().__init__()

    def set_method(self, name, function_mock: Callable):
        """
        set the method `name` to be `function_mock`
        this is intended to be used on testing to mock a response from a hook
        """
        self.__dict__[name] = function_mock
        return self


class TestHookOperator(BaseOperator):
    template_fields = ["fn_kwargs"]

    def __init__(
        self, execute_fn: Callable, fn_kwargs=None, task_id="test_hook", **kwargs
    ):
        self.execute_fn = execute_fn
        self.fn_kwargs = fn_kwargs or {}

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        self.execute_fn(context, **self.fn_kwargs)

pgsql_hook = PostgreSQLConnectorHook(rdbms_conn_id="connection_pgsql_datalake")

def test_get_first_record(context, pgsql_hook:DbApiHookInterface, dql_statement: str):
    first_record = pgsql_hook.get_records(sql=dql_statement)
    logging.info(f"valor: {first_record}")
    logging.info(f"tipo: {type(first_record)}")
    assert (
        first_record != None
    )

with test_dag("test_conexao_pgsql_datalake") as dag:
    
    test_first_record = TestHookOperator(
        task_id="test_first_record", execute_fn=test_get_first_record
    )
    
    get_first_record_query = f" SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'cobranca' AND table_name = 'money_on_table'"
    test_first_record.fn_kwargs = {
        "pgsql_hook": pgsql_hook,
        "dql_statement": get_first_record_query
    }