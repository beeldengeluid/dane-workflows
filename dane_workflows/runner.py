import sys
from dane_workflows.util.base_util import import_dane_workflow_module
from dane_workflows.task_scheduler import TaskScheduler


def construct_task_scheduler(config) -> TaskScheduler:
    if _validate_config(config):
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
    print("Invalid config, quitting")
    sys.exit()


def _validate_config(config) -> bool:
    try:
        required_components = [
            "LOGGING",
            "TASK_SCHEDULER",
            "STATUS_HANDLER",
            "DATA_PROVIDER",
            "PROC_ENV",
            "EXPORTER",
        ]
        assert all(
            component in config for component in required_components
        ), f"Error one or more {required_components} missing in config"

        # check if the optional status monitor is there
        if "STATUS_MONITOR" in config:
            required_components.append("STATUS_MONITOR")

        # some components MUST have a TYPE defined
        for component in required_components:
            if component in ["TASK_SCHEDULER", "LOGGING"]:  # no TYPE needed for these
                continue
            assert "TYPE" in config[component], f"{component}.TYPE missing"
    except AssertionError as e:
        print(e)
        return False
    return True
