import logging
from typing import Type, Optional

from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from openlineage.airflow.extractors.great_expectations_extractor import GreatExpectationsExtractor
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor


_extractors = [
    PostgresExtractor,
    BigQueryExtractor,
    GreatExpectationsExtractor,
    SnowflakeExtractor
]

_patchers = [
    GreatExpectationsExtractor
]


class Extractors:
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """
    def __init__(self):
        # Do not expose extractors relying on external dependencies that are not installed
        self.extractors = {
            extractor.operator_class: extractor
            for extractor
            in _extractors
            if getattr(extractor, 'operator_class', None) is not None
        }

        self.patchers = {
            extractor.operator_class: extractor
            for extractor
            in _patchers
            if getattr(extractor, 'operator_class', None) is not None
        }

    def get_extractor_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        logging.getLogger().info(f"{name} - \n{self.extractors}")
        if name in self.extractors:
            return self.extractors[name]
        return None

    def get_patcher_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.patchers:
            return self.patchers[name]
        return None
