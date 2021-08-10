import logging
from typing import Union, Optional, List

from airflow.lineage.backend import LineageBackend
from openlineage.airflow.extractors.extractors import Extractors
from openlineage.airflow.extractors.base import StepMetadata, BaseExtractor


class OpenLineageBackend(LineageBackend):
    def __init__(self):
        self.extractors = {}
        self.extractor_mapper = Extractors()
        self.log = logging.getLogger()

    def send_lineage(
        self,
        operator=None,
        inlets=None,
        outlets=None,
        context=None
    ):
        self.log.info(f"{operator}\n{inlets}\n{outlets}\n{context}\n")
        self.log.info(
            f"""Metadata: {self._extract_metadata(
                dag_id=context['dag'].dag_id,
                dagrun=context['dag_run'],
                task=operator,
                task_instance=context['task_instance']
            )}"""
        )

    def _extract_metadata(self, dag_id, dagrun, task, task_instance=None) -> StepMetadata:
        extractor = self._get_extractor(task)
        task_info = f'task_type={task.__class__.__name__} ' \
            f'airflow_dag_id={dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '
        if extractor:
            try:
                self.log.debug(
                    f'Using extractor {extractor.__class__.__name__} {task_info}')
                step = self._extract(extractor, task_instance)

                if isinstance(step, StepMetadata):
                    return step

                # Compatibility with custom extractors
                if isinstance(step, list):
                    if len(step) == 0:
                        return StepMetadata(
                            name=self._openlineage_job_name(dag_id, task.task_id)
                        )
                    elif len(step) >= 1:
                        self.log.warning(
                            f'Extractor {extractor.__class__.__name__} {task_info} '
                            f'returned more then one StepMetadata instance: {step} '
                            f'will drop steps except for first!'
                        )
                    return step[0]

            except Exception as e:
                self.log.exception(
                    f'Failed to extract metadata {e} {task_info}',
                )
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')

    def _extract(self, extractor, task_instance) -> \
            Union[Optional[StepMetadata], List[StepMetadata]]:
        if task_instance:
            step = extractor.extract_on_complete(task_instance)
            if step:
                return step

        return extractor.extract()

    def _get_extractor(self, task) -> Optional[BaseExtractor]:
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.extractor_mapper.get_extractor_class(task.__class__)
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None

    @classmethod
    def _openlineage_job_name_from_task_instance(cls, task_instance):
        return cls._openlineage_job_name(task_instance.dag_id, task_instance.task_id)

    @staticmethod
    def _openlineage_job_name(dag_id: str, task_id: str) -> str:
        return f'{dag_id}.{task_id}'
