from dane_workflows.util.base_util import import_dane_workflow_module
from dane_workflows.task_scheduler import TaskScheduler


def construct_task_scheduler(config) -> TaskScheduler:
    return TaskScheduler(
        config,
        import_dane_workflow_module(config["STATUS_HANDLER"]["TYPE"]),
        import_dane_workflow_module(config["DATA_PROVIDER"]["TYPE"]),
        import_dane_workflow_module(config["PROC_ENV"]["TYPE"]),
        import_dane_workflow_module(config["EXPORTER"]["TYPE"]),
        import_dane_workflow_module(config["STATUS_MONITOR"]["TYPE"])
        if "STATUS_MONITOR" in config
        else None,
    )
