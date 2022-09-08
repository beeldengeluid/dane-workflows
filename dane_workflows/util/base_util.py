import os
import sys
from argparse import ArgumentParser, Namespace
import logging
from logging.handlers import TimedRotatingFileHandler
from yaml import load, FullLoader
from yaml.scanner import ScannerError
from pathlib import Path
from importlib import import_module
from typing import Optional, Tuple


# Call this first thing in your main.py to extract the default CMD line options and config YAML
def extract_exec_params() -> Optional[Tuple[dict, Namespace]]:
    parser = ArgumentParser(description="DANE workflow")
    parser.add_argument("--cfg", action="store", dest="cfg", default="config.yml")
    parser.add_argument("--opt", action="store", dest="opt", default=None)
    args = parser.parse_args()
    print(f"Got the following CMD line arguments: {args}")
    return load_config_or_die(args.cfg), args


# since the config is vital, it should be available
def load_config_or_die(cfg_file: str):
    print(f"Going to load the following config: {cfg_file}")
    try:
        with open(cfg_file, "r") as yamlfile:
            return load(yamlfile, Loader=FullLoader)
    except (FileNotFoundError, ScannerError) as e:
        print(f"Not a valid file path or config file {cfg_file}")
        print(e)
        sys.exit()


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
    print(f"Trying to automatically create dir: {path}")
    if not os.path.exists(get_parent_dir(path)):
        print(f"Error: cannot automatically create {path}; parent dir does not exist")
        return False
    if not os.path.exists(path):
        print(f"Dir: '{path}' does not exist, creating it...")
        try:
            os.makedirs(path)
        except OSError:
            print(f"OSError {path} could not be created...")
            return False
    return True


def import_dane_workflow_module(module_path: str):
    tmp = module_path.split(".")
    if len(tmp) != 3:
        print(f"Malconfigured module path: {module_path}")
        sys.exit()
    # module = getattr(__import__(tmp[0]), tmp[1])
    module = import_module(f"{tmp[0]}.{tmp[1]}")
    workflow_class = getattr(module, tmp[2])
    # globals()[tmp[2]] = workflow_class
    return workflow_class


def init_logger(config):
    log_conf = config["LOGGING"]
    logger = logging.getLogger(log_conf["NAME"])
    logger.setLevel(log_conf["LEVEL"])
    # create file handler which logs to file
    if not os.path.exists(os.path.realpath(log_conf["DIR"])):
        os.makedirs(os.path.realpath(log_conf["DIR"]), exist_ok=True)

    fh = TimedRotatingFileHandler(
        os.path.join(os.path.realpath(log_conf["DIR"]), "dane-workflows.log"),
        when="W6",  # start new log on sunday
        backupCount=3,
    )
    fh.setLevel(log_conf["LEVEL"])
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(log_conf["LEVEL"])
    # create formatter and add it to the handlers
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    """
    formatter = logging.Formatter(
        "%(asctime)s|%(levelname)s|%(process)d|%(module)s|%(funcName)s|%(lineno)d|%(message)s"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_logger(config):
    return logging.getLogger(config["LOGGING"]["NAME"])
