import argparse
import sys
from dane_workflows.util.base_util import load_config
from dane_workflows.runner import construct_task_scheduler

# test a full workflow
if __name__ == "__main__":
    # first determine which config file to use
    parser = argparse.ArgumentParser(
        description="dane-workflows default start-up script"
    )
    parser.add_argument("--cfg", action="store", dest="cfg", default="config.yml")
    args = parser.parse_args()
    print(f"Going to load the following config: {args.cfg}")

    # now go ahead and load the config
    config = load_config(args.cfg)
    if config is None:
        print(f"Not a valid file path or config file {args.cfg}")
        sys.exit()

    # obtain the runner, i.e. TaskScheduler
    runner = construct_task_scheduler(config)
    runner.run()

    print("All done")
