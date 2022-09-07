import argparse
import sys
from pathlib import Path
from dane_workflows.util.base_util import load_config, import_module
from dane_workflows.task_scheduler import TaskScheduler

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

    """ TODO Move auto creation of dirs to _validate_config functions of each component
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
    if not init_data_dirs(data_dirs):
        print(
            "Could not create all the necessary data dirs to start-up this workflow, quitting"
        )
        sys.exit()
    """

    ts = TaskScheduler(
        config,
        import_module(config["STATUS_HANDLER"]["TYPE"]),
        import_module(config["DATA_PROVIDER"]["TYPE"]),
        import_module(config["PROC_ENV"]["TYPE"]),
        import_module(config["EXPORTER"]["TYPE"]),
        import_module(config["STATUS_MONITOR"]["TYPE"]),
    )

    ts.run()

    print("All done")
