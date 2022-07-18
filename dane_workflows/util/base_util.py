import os
import logging
from logging.handlers import TimedRotatingFileHandler
from yaml import load, FullLoader
from yaml.scanner import ScannerError
from pathlib import Path


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


def load_config(cfg_file):
    try:
        with open(cfg_file, "r") as yamlfile:
            return load(yamlfile, Loader=FullLoader)
    except (FileNotFoundError, ScannerError) as e:
        print(e)
    return None


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
