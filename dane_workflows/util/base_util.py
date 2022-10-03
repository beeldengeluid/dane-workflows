import os
import sys
from argparse import ArgumentParser, Namespace
import logging
from yaml import load, FullLoader
from yaml.scanner import ScannerError
from pathlib import Path
from importlib import import_module
from typing import Optional, Tuple


logger = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s|%(levelname)s|%(process)d|%(module)s|%(funcName)s|%(lineno)d|%(message)s"


# Call this first thing in your main.py to extract the default CMD line options and config YAML
def extract_exec_params() -> Optional[Tuple[dict, Namespace]]:
    parser = ArgumentParser(description="DANE workflow")
    parser.add_argument("--cfg", action="store", dest="cfg", default="config.yml")
    parser.add_argument("--log", action="store", dest="loglevel", default="DEBUG")
    parser.add_argument("--opt", action="store", dest="opt", default=None)
    args = parser.parse_args()

    # load the config and validate it
    config = load_config_or_die(args.cfg)

    # init the file logger
    logger.info(f"Got the following CMD line arguments: {args}")
    logger.info(f"Succesfully loaded & validated {args.cfg}")
    return config, args


# since the config is vital, it should be available
def load_config_or_die(cfg_file: str):
    logger.info(f"Going to load the following config: {cfg_file}")
    try:
        with open(cfg_file, "r") as yamlfile:
            config = load(yamlfile, Loader=FullLoader)
            if validate_config(config):
                return config
            else:
                logger.critical(f"Config: {cfg_file} invalid, quitting")
                sys.exit()
    except (FileNotFoundError, ScannerError):
        logger.exception(f"Not a valid file path or config file {cfg_file}")
        sys.exit()


def validate_config(config) -> bool:
    try:
        required_components = [
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
            if component in ["TASK_SCHEDULER"]:  # no TYPE needed for these
                continue
            assert "TYPE" in config[component], f"{component}.TYPE missing"

    except AssertionError:
        logger.exception("Invalid config YAML")
        return False
    return True


# returns the root of this repo by running "cd ../.." from this __file__ on
def get_repo_root() -> str:
    return os.path.realpath(
        os.path.join(os.path.dirname(__file__), os.sep.join(["..", ".."]))
    )


# see https://stackoverflow.com/questions/52878999/adding-a-relative-path-to-an-absolute-path-in-python
def relative_from_repo_root(path: str) -> str:
    return os.path.normpath(
        os.path.join(
            get_repo_root(),
            path.replace("/", os.sep),  # POSIX path seperators also work on windows
        )
    )


def to_abs_path(f) -> str:
    return os.path.realpath(os.path.dirname(f))


def relative_from_file(f, path: str) -> str:
    return os.path.normpath(
        os.path.join(
            to_abs_path(f),
            path.replace("/", os.sep),  # POSIX path seperators also work on windows
        )
    )


def check_setting(setting, t, optional=False):
    return (type(setting) == t and optional is False) or (
        optional and (setting is None or type(setting) == t)
    )


def check_log_level(level: str) -> bool:
    return level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def validate_parent_dirs(paths: list):
    try:
        for p in paths:
            assert os.path.exists(
                Path(p).parent.absolute()
            ), f"Parent dir of file does not exist: {Path(p).parent.absolute()}"
    except AssertionError as e:
        raise (e)


def validate_file_paths(paths: list):
    try:
        os.getcwd()  # why is this called again?
        for p in paths:
            assert os.path.exists(p), f"File does not exist: { Path(p).absolute()}"
    except AssertionError as e:
        raise (e)


def get_parent_dir(path: str) -> Path:
    return Path(path).parent


# the parent dir of the configured directory has to exist for this to work
def auto_create_dir(path: str) -> bool:
    logger.info(f"Trying to automatically create dir: {path}")
    if not os.path.exists(get_parent_dir(path)):
        logger.error(
            f"Error: cannot automatically create {path}; parent dir does not exist"
        )
        return False
    if not os.path.exists(path):
        logger.info(f"Dir: '{path}' does not exist, creating it...")
        try:
            os.makedirs(path)
        except OSError:
            logger.exception(f"OSError {path} could not be created...")
            return False
    return True


def import_dane_workflow_class(class_path: str):
    tmp = class_path.split(".")
    if len(tmp) < 2:  # always specify modulepath.class
        logger.critical(f"Malconfigured module path: {class_path}")
        sys.exit()
    module_path = ".".join(tmp[:-1])
    try:
        module = import_module(f"{module_path}")
        workflow_class = getattr(module, tmp[-1])  # last element is the class name
        return workflow_class
    except ModuleNotFoundError:
        logger.exception("Module path incorrectly configured")
    except AttributeError:
        logger.exception("Module class incorrectly configured")
    return None
