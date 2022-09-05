import argparse
import os
import sys
from pathlib import Path
from typing import List
from dane_workflows.util.base_util import load_config
from dane_workflows.task_scheduler import TaskScheduler
from dane_workflows.status import SQLiteStatusHandler
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.data_processing import ExampleDataProcessingEnvironment, DANEEnvironment
from dane_workflows.exporter import ExampleExporter

def __init_data_dirs(data_dirs: List[str]) -> bool:
    print("Checking if DATA_DIR exists")
    for path in data_dirs:
        if not os.path.exists(path):
            print(f"path: '{path}' does not exist, creating it...")
            try:
                os.makedirs(path)
            except OSError:
                print(f"OSError {path} could not be created...")
                return False
    return True

# test a full workflow
if __name__ == "__main__":

     # first determine which config file to use
    parser = argparse.ArgumentParser(description="Short sample app")
    parser.add_argument("--cfg", action="store", dest="cfg", default="config.yml")
    args = parser.parse_args()
    print(f"Going to load the following config: {args.cfg}")

    # now go ahead and load the config
    config = load_config(args.cfg)
    if config is None:
        print(f"Not a valid file path or config file {args.cfg}")
        sys.exit()

    dp_conf = config["DATA_PROVIDER"]["CONFIG"]
    pe_conf = config["PROC_ENV"]["CONFIG"] if "CONFIG" in config["PROC_ENV"] else None
    sh_conf = (
        config["STATUS_HANDLER"]["CONFIG"]
        if "CONFIG" in config["STATUS_HANDLER"]
        else None
    )

    # assemble all data dirs from the config.yml
    data_dirs = [dp_conf["DATA_DIR"]] if "DATA_DIR" in dp_conf else []
    if sh_conf and "DB_FILE" in sh_conf:
        data_dirs.append(Path(sh_conf["DB_FILE"]).parent)
    if pe_conf and "DANE_STATUS_DIR" in pe_conf:
        data_dirs.append(pe_conf["DANE_STATUS_DIR"])

    # make sure they all exist before continuing
    if not __init_data_dirs(data_dirs):
        print(
            "Could not create all the necessary data dirs to start-up this workflow, quitting"
        )
        sys.exit()

    ts = TaskScheduler(
        config,
        SQLiteStatusHandler,
        ExampleDataProvider,
        DANEEnvironment,  # ExampleDataProcessingEnvironment,
        ExampleExporter,
    )

    ts.run()

    print("All done")
