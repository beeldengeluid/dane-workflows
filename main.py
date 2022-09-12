from dane_workflows.util.base_util import extract_exec_params, LOG_FORMAT
from dane_workflows.runner import construct_task_scheduler
import logging
import sys

"""
Example main execution script for your (DANE) workflow:

By defualt the following CMD line params are supported:
* --cfg=./path_to_your/config.yml (default="config.yml")
* --log=DEBUG|INFO|WARNING|ERROR|CRITICAL (default="DEBUG")
* --opt=anything-you-like (default=None)
"""

# initialises the root logger
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,  # configure a stream handler only for now (single handler)
    format=LOG_FORMAT,
)
logger = logging.getLogger()

if __name__ == "__main__":
    logger.info("Starting example workflow")
    config, cmd_args = extract_exec_params()

    # setting the loglevel
    log_level = cmd_args.loglevel.upper()
    logger.info(f"Setting the log level to: {log_level}")
    logger.setLevel(log_level)

    # insert custom behaviour e.g. before running the workflow
    if cmd_args.opt == "anything-you-like":
        logger.info(f"Executing custom code block using --opt={cmd_args.opt}")

    # obtain the runner, i.e. TaskScheduler
    runner = construct_task_scheduler(config)
    runner.run()

    logger.info("All done")
