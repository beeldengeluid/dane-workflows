from dane_workflows.util.base_util import import_dane_workflow_class
from dane_workflows.task_scheduler import TaskScheduler
import logging


logger = logging.getLogger(__name__)


# This function takes the complexity of configuring a TaskScheduler away from the library user
# A user can simply import this module's function to spin up a workflow with a config
def construct_task_scheduler(config) -> TaskScheduler:
    logger.info("Constructing a TaskScheduler (a.k.a runner) from the config...")
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
