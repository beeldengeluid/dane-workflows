import logging
from logging.handlers import TimedRotatingFileHandler
from mockito import when, ANY
import os
import pytest
from dane_workflows.util.base_util import (
    get_repo_root,
    relative_from_repo_root,
    relative_from_file,
    check_setting,
    check_log_level,
    validate_parent_dirs,
    validate_file_paths,
    load_config,
    init_logger,
)


def test_get_repo_root():
    # expect a path that is 2 levels up from this location
    created_path = get_repo_root()

    assert os.path.exists(created_path)
    assert created_path.endswith(os.sep.join(["dane-workflows"]))


def test_relative_from_repo_root():
    # create a path with the relative path to folder with this test
    created_path = relative_from_repo_root(os.sep.join(["tests", "unit_tests"]))

    assert os.path.exists(created_path)
    assert created_path == os.path.dirname(__file__)


def test_relative_from_file():
    # create the path to base_util from the path to this file
    created_path = relative_from_file(__file__, "../../dane_workflows/util")

    assert os.path.exists(created_path)
    assert created_path.endswith("util")


@pytest.mark.parametrize(
    ("setting", "desired_type", "optional", "expected_result"),
    [
        ("my-setting", str, False, True),
        (1, int, False, True),
        ((1 / 3), float, False, True),
        (["my-setting"], list, False, True),
        ({"my-key": "my-setting"}, dict, False, True),
        ("my-setting", int, False, False),
        (1, str, False, False),
        ((1 / 3), int, False, False),
        (["my-setting"], dict, False, False),
        ({"my-key": "my-setting"}, list, False, False),
        (None, int, False, False),
        (None, str, False, False),
        (None, int, False, False),
        (None, dict, False, False),
        (None, list, False, False),
        ("my-setting", str, True, True),
        (1, int, True, True),
        ((1 / 3), float, True, True),
        (["my-setting"], list, True, True),
        ({"my-key": "my-setting"}, dict, True, True),
        (None, str, True, True),
        (None, int, True, True),
        (None, float, True, True),
        (None, list, True, True),
        (None, dict, True, True),
        ("my-setting", int, True, False),
        (1, str, True, False),
        ((1 / 3), int, True, False),
        (["my-setting"], dict, True, False),
        ({"my-key": "my-setting"}, list, True, False),
    ],
)
def test_check_setting(setting, desired_type, optional, expected_result):
    assert check_setting(setting, desired_type, optional=optional) == expected_result


@pytest.mark.parametrize(
    "log_level, expected_result",
    [
        ("DEBUG", True),
        ("INFO", True),
        ("WARNING", True),
        ("ERROR", True),
        ("CRITICAL", True),
        ("hog wash", False),
        ("critical", False),
    ],
)
def test_check_log_level(log_level, expected_result):
    assert check_log_level(log_level) == expected_result


@pytest.mark.parametrize(
    ("paths", "should_pass_test"),
    [
        ([__file__], True),
        ([get_repo_root()], True),
        ([os.getcwd()], True),
        ([__file__, get_repo_root()], True),
        (
            [os.sep.join([os.getcwd(), "nonsense"])],
            True,
        ),  # ok as we are checking that the parent exists
        (
            [os.sep.join([__file__, "nonsense"])],
            True,
        ),  # ok as we are checking that the parent exists
        ([os.sep.join(["nonsense", "rubbish"])], False),
        ([os.sep.join([os.getcwd(), "nonsense", "rubbish"])], False),
        ([get_repo_root(), os.sep.join([os.getcwd(), "nonsense", "rubbish"])], False),
        (
            [
                get_repo_root(),
                os.sep.join([os.getcwd(), "nonsense", "rubbish"]),
                os.getcwd(),
            ],
            False,
        ),
    ],
)
def test_validate_parent_dirs(paths, should_pass_test):
    if should_pass_test:
        validate_parent_dirs(paths)
    else:
        with pytest.raises(AssertionError):
            validate_parent_dirs(paths)


@pytest.mark.parametrize(
    ("paths", "should_pass_test"),
    [
        ([__file__], True),
        ([get_repo_root()], True),
        ([os.getcwd()], True),
        ([__file__, get_repo_root()], True),
        ([os.sep.join([os.getcwd(), "nonsense"])], False),
        ([os.sep.join([__file__, "nonsense"])], False),
        ([os.sep.join(["nonsense", "rubbish"])], False),
        ([os.sep.join([os.getcwd(), "nonsense", "rubbish"])], False),
        ([get_repo_root(), os.sep.join([os.getcwd(), "nonsense", "rubbish"])], False),
        (
            [
                get_repo_root(),
                os.sep.join([os.getcwd(), "nonsense", "rubbish"]),
                os.getcwd(),
            ],
            False,
        ),
    ],
)
def test_validate_file_paths(paths, should_pass_test):
    if should_pass_test:
        validate_file_paths(paths)
    else:
        with pytest.raises(AssertionError):
            validate_file_paths(paths)


@pytest.mark.parametrize(
    ("path_to_file", "expect_file"),
    [
        (relative_from_file(__file__, "../../config-unit-test.yml"), True),
        (os.sep.join([os.getcwd(), "nonsense"]), False),
        (__file__, False),
    ],
)
def test_load_config(path_to_file, expect_file):
    if expect_file:
        assert load_config(path_to_file)
    else:
        assert not load_config(path_to_file)


def test_init_logger(config):

    with when(os.path).exists(ANY).thenReturn(True):
        logger = init_logger(config)

        assert logger
        assert len(logger.handlers) == 2
        assert isinstance(logger.handlers[0], TimedRotatingFileHandler)
        assert (
            logging.getLevelName(logger.handlers[0].level) == config["LOGGING"]["LEVEL"]
        )
        assert isinstance(logger.handlers[1], logging.StreamHandler)
        assert (
            logging.getLevelName(logger.handlers[1].level) == config["LOGGING"]["LEVEL"]
        )
