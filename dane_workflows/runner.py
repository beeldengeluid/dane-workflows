from dane_workflows.util.base_util import import_dane_workflow_class
from dane_workflows.task_scheduler import TaskScheduler
import logging


logger = logging.getLogger(__name__)


def construct_task_scheduler(config) -> TaskScheduler:
    return TaskScheduler(
        config,
        import_dane_workflow_class(config["STATUS_HANDLER"]["TYPE"]),
        import_dane_workflow_class(config["DATA_PROVIDER"]["TYPE"]),
        import_dane_workflow_class(config["PROC_ENV"]["TYPE"]),
        import_dane_workflow_class(config["EXPORTER"]["TYPE"]),
        import_dane_workflow_class(config["STATUS_MONITOR"]["TYPE"])
        if "STATUS_MONITOR" in config
        else None,
    )
