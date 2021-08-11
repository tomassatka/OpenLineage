import logging
import uuid
import time
from typing import Union, Optional, List

from airflow.lineage.backend import LineageBackend

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors.extractors import Extractors
from openlineage.airflow.extractors.base import StepMetadata, BaseExtractor
from openlineage.airflow.utils import DagUtils, get_location, get_custom_facets


class OpenLineageBackend(LineageBackend):
    def __init__(self):
        self.extractors = {}
        self.extractor_mapper = Extractors()
        self.log = logging.getLogger()
        self.adapter = OpenLineageAdapter()

    def send_lineage(
        self,
        operator=None,
        inlets=None,
        outlets=None,
        context=None
    ):
        self.log.info(f"{operator}\n{inlets}\n{outlets}\n{context}\n")
        dag = context['dag']
        dagrun = context['dag_run']
        task_instance = context['task_instance']

        run_id = str(uuid.uuid4())
        job_name = self._openlineage_job_name(dag.dag_id, operator.task_id)

        step = self._extract_metadata(
            dag_id=dag.dag_id,
            dagrun=dagrun,
            task=operator,
            task_instance=context['task_instance']
        )

        self.adapter.start_task(
            run_id=run_id,
            job_name=job_name,
            job_description=dag.description,
            event_time=DagUtils.to_iso_8601(self._now_ms()),
            parent_run_id=dagrun.run_id,
            code_location=self._get_location(operator),
            nominal_start_time=DagUtils.get_start_time(dagrun.execution_date),
            nominal_end_time=DagUtils.get_end_time(
                dagrun.execution_date,
                dag.following_schedule(dagrun.execution_date)
            ),
            step=step,
            run_facets={**step.run_facets, **get_custom_facets(operator, dagrun.external_trigger)}
        )

        self.adapter.complete_task(
            run_id=run_id,
            job_name=job_name,
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            step=step
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

    @staticmethod
    def _get_location(task):
        try:
            if hasattr(task, 'file_path') and task.file_path:
                return get_location(task.file_path)
            else:
                return get_location(task.dag.fileloc)
        except Exception:
            return None

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))
