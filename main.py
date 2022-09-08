from dane_workflows.util.base_util import extract_exec_params
from dane_workflows.runner import construct_task_scheduler

"""
Example main execution script for your (DANE) workflow:

By defualt the following CMD line params are supported:
* --cfg=./path_to_your/config.yml (default="config.yml")
* --opt=anything-you-like (default=None)
"""
if __name__ == "__main__":
    config, cmd_args, logger = extract_exec_params()

    # insert custom behaviour e.g. before running the workflow
    if cmd_args.opt == "anything-you-like":
        logger.info("Well I never!")

    # obtain the runner, i.e. TaskScheduler
    runner = construct_task_scheduler(config)
    runner.run()

    logger.info("All done")
