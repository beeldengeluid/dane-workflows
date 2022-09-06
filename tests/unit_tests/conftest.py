import os
import pytest
from dane_workflows.util.base_util import load_config, relative_from_file


WORKFLOW_ROOT_DIR = relative_from_file(__file__, os.sep.join(["..", ".."]))


@pytest.fixture()
def config():
    config = load_config(relative_from_file(__file__, "../../config-unit-test.yml"))

    # adjust paths as these are relative
    if (
        "STATUS_HANDLER" in config
        and "CONFIG" in config["STATUS_HANDLER"]
        and "DB_FILE" in config["STATUS_HANDLER"]["CONFIG"]
    ):

        config["STATUS_HANDLER"]["CONFIG"]["DB_FILE"] = os.sep.join(
            [WORKFLOW_ROOT_DIR, config["STATUS_HANDLER"]["CONFIG"]["DB_FILE"]]
        )
    return config


@pytest.fixture
def slack_monitor_config():
    config = load_config(relative_from_file(__file__, "../../config-unit-test.yml"))
    config["STATUS_MONITOR"]["CONFIG"] = {}
    config["STATUS_MONITOR"]["CONFIG"]["TOKEN"] = "some_random_token"
    config["STATUS_MONITOR"]["CONFIG"]["CHANNEL"] = "a_channel"
    config["STATUS_MONITOR"]["CONFIG"]["WORKFLOW_NAME"] = "TESTING"
    config["STATUS_MONITOR"]["CONFIG"]["INCLUDE_EXTRA_INFO"] = False
    return config
