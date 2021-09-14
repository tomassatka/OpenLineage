import importlib
import os

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
        self.extractors = {}
        self.patchers = {}

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        for patcher in _patchers:
            for operator_class in patcher.get_operator_classnames():
                self.patchers[operator_class] = patcher

        for key, value in os.environ.items():
            if key.startswith("OPENLINEAGE_EXTRACTOR_"):
                operator = key[22:]
                parts = value.split('.')
                module = '.'.join(parts[:-1])
                extractor = importlib.import_module(module)
                self.extractors[operator] = getattr(extractor, parts[-1])

    def add_extractor(self, operator: str, extractor: Type):
        self.extractors[operator] = extractor

    def get_extractor_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]
        return None

    def get_patcher_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.patchers:
            return self.patchers[name]
        return None
