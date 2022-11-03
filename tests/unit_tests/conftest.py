import os
import pytest
from dane_workflows.util.base_util import load_config_or_die, relative_from_file


WORKFLOW_ROOT_DIR = relative_from_file(__file__, os.sep.join(["..", ".."]))


@pytest.fixture()
def config():
    config = load_config_or_die(
        relative_from_file(__file__, "../../config-unit-test.yml")
    )

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
    config = load_config_or_die(
        relative_from_file(__file__, "../../config-unit-test.yml")
    )
    config["STATUS_MONITOR"]["CONFIG"] = {}
    config["STATUS_MONITOR"]["CONFIG"]["TOKEN"] = "some_random_token"
    config["STATUS_MONITOR"]["CONFIG"]["CHANNEL"] = "a_channel"
    config["STATUS_MONITOR"]["CONFIG"]["WORKFLOW_NAME"] = "TESTING"
    config["STATUS_MONITOR"]["CONFIG"]["INCLUDE_EXTRA_INFO"] = False
    return config


@pytest.fixture
def dane_data_processing_config():
    config = load_config_or_die(
        relative_from_file(__file__, "../../config-unit-test.yml")
    )
    config["PROC_ENV"]["TYPE"] = "dane_workflows.data_processing.DANEEnvironment"
    config["PROC_ENV"]["CONFIG"] = {}
    config["PROC_ENV"]["CONFIG"]["DANE_HOST"] = "your-dane-host"
    config["PROC_ENV"]["CONFIG"]["DANE_ES_HOST"] = "your-dane-es-host"
    config["PROC_ENV"]["CONFIG"]["DANE_ES_PORT"] = 80
    config["PROC_ENV"]["CONFIG"]["DANE_ES_INDEX"] = "your-dane-index"
    config["PROC_ENV"]["CONFIG"]["DANE_TASK_ID"] = "DOWNLOAD"
    config["PROC_ENV"]["CONFIG"]["DANE_STATUS_DIR"] = "some-status-dir"
    config["PROC_ENV"]["CONFIG"]["DANE_MONITOR_INTERVAL"] = 3
    config["PROC_ENV"]["CONFIG"]["DANE_BATCH_PREFIX"] = "dummy"
    config["PROC_ENV"]["CONFIG"]["DANE_ES_QUERY_TIMEOUT"] = 20

    return config


@pytest.fixture
def example_processing_config():
    config = load_config_or_die(
        relative_from_file(__file__, "../../config-unit-test.yml")
    )
    config["PROC_ENV"]["TYPE"] = "dane_workflows.data_processing.ExampleDataProcessingEnvironment"
    config["PROC_ENV"]["CONFIG"] = {}
    config["PROC_ENV"]["CONFIG"]["EXAMPLE_KEY"] = "example_value"
    return config


@pytest.fixture
def example_exporter_config():
    config = load_config_or_die(
            relative_from_file(__file__, "../../config-unit-test.yml")
            )
    config["EXPORTER"]["TYPE"] = "dane_workflows.exporter.ExampleExporter"
    config["EXPORTER"]["DAAN_ES_HOST"] = "dummy_es_host"
    config["EXPORTER"]["DAAN_ES_PORT"] = 0
    config["EXPORTER"]["DAAN_ES_OUTPUT_INDEX"] = "dummy_es_input_index"
    config["EXPORTER"]["DAAN_ES_INPUT_INDEX"] = "dummy_es_output_index"
    return config
