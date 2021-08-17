from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.postgres_operator import PostgresOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from openlineage.airflow.extractors import Extractors


def test_all_extractors():
    extractors = [
        PostgresOperator,
        BigQueryOperator,
        BigQueryExecuteQueryOperator,
        GreatExpectationsOperator,
        SnowflakeOperator
    ]

    assert len(Extractors().extractors) == len(extractors)

    for extractor in extractors:
        assert Extractors().get_extractor_class(extractor)
