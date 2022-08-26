import os
import pytest
from mockito import when, verify, spy2, ANY, unstub
from dane_workflows.task_scheduler import TaskScheduler
import dane_workflows.data_provider
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.data_processing import (
    ExampleDataProcessingEnvironment,
)
from dane_workflows.exporter import ExampleExporter
from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
)
from test_util import new_batch, LoggerMock


@pytest.mark.parametrize(
    "error",
    [
        None,
        "no_logging_config",
        "no_log_name",
        "no_log_dir",
        "no_log_level",
        "no_ts_config",
        "no_ts_batch_size",
        "no_ts_batch_prefix",
        "bad_logging_dir",
    ],
)
def test_validate_config(config, error):
    if error == "no_logging_config":
        del config["LOGGING"]
    elif error == "no_log_name":
        del config["LOGGING"]["NAME"]
    elif error == "no_log_dir":
        del config["LOGGING"]["DIR"]
    elif error == "no_log_level":
        del config["LOGGING"]["LEVEL"]
    elif error == "no_ts_config":
        del config["TASK_SCHEDULER"]
    elif error == "no_ts_batch_size":
        del config["TASK_SCHEDULER"]["BATCH_SIZE"]
    elif error == "no_ts_batch_prefix":
        del config["TASK_SCHEDULER"]["BATCH_PREFIX"]
    elif error == "bad_logging_dir":
        config["LOGGING"]["DIR"] = os.sep.join([os.getcwd(), "nonsense", "logging"])

    if error:
        with pytest.raises(SystemExit):
            TaskScheduler(
                config,
                ExampleStatusHandler,
                ExampleDataProvider,
                ExampleDataProcessingEnvironment,
                ExampleExporter,
                unit_test=True,
            )
    else:
        try:
            spy2(dane_workflows.util.base_util.check_log_level)
            spy2(dane_workflows.util.base_util.check_setting)
            spy2(dane_workflows.util.base_util.validate_parent_dirs)

            TaskScheduler(
                config,
                ExampleStatusHandler,
                ExampleDataProvider,
                ExampleDataProcessingEnvironment,
                ExampleExporter,
                unit_test=True,
            )

            verify(dane_workflows.util.base_util, times=1).check_log_level(ANY)
            verify(dane_workflows.util.base_util, times=4).check_setting(ANY, ANY)
            verify(dane_workflows.util.base_util, times=1).validate_parent_dirs(ANY)
        finally:
            unstub()


@pytest.mark.parametrize(  # TODO test & support empty proc_batch
    ("proc_batch", "proc_batch_id", "proc_env_success", "success"),
    [
        (new_batch(0, ProcessingStatus.NEW), 0, True, True),
        (new_batch(1, ProcessingStatus.NEW), 1, True, True),
        (new_batch(0, ProcessingStatus.NEW), 0, False, False),
    ],
)
def test_register_proc_batch(
    config, proc_batch, proc_batch_id, proc_env_success, success
):
    logger_mock = LoggerMock()  # mock the logger to avoid log file output
    with when(dane_workflows.util.base_util).init_logger(config).thenReturn(
        logger_mock
    ), when(  # mock success/failure by returning empty status_rows or ones with proper status_rows
        ExampleDataProcessingEnvironment
    ).register_batch(
        proc_batch_id, proc_batch
    ).thenReturn(
        new_batch(0, ProcessingStatus.BATCH_REGISTERED) if proc_env_success else None
    ):
        spy2(logger_mock.info)
        spy2(logger_mock.error)

        ts = TaskScheduler(
            config,
            ExampleStatusHandler,
            ExampleDataProvider,
            ExampleDataProcessingEnvironment,
            ExampleExporter,
            unit_test=True,
        )

        assert ts._register_proc_batch(proc_batch_id, proc_batch) == success
        verify(ExampleDataProcessingEnvironment, times=1).register_batch(
            proc_batch_id, ANY
        )
        verify(logger_mock, times=2 if proc_env_success else 1).info(ANY)
        verify(logger_mock, times=0 if proc_env_success else 1).error(ANY)


@pytest.mark.parametrize(
    ("proc_batch_id", "proc_env_success", "success"),
    [
        (0, True, True),
        (0, False, False),
    ],
)
def test_process_proc_batch(config, proc_batch_id, proc_env_success, success):
    logger_mock = LoggerMock()
    with when(dane_workflows.util.base_util).init_logger(config).thenReturn(
        logger_mock
    ), when(  # mock success/failure by returning empty status_rows or ones with proper status_rows
        ExampleDataProcessingEnvironment
    ).process_batch(
        proc_batch_id
    ).thenReturn(
        new_batch(0, ProcessingStatus.PROCESSING) if proc_env_success else None
    ):
        spy2(logger_mock.info)
        spy2(logger_mock.error)

        ts = TaskScheduler(
            config,
            ExampleStatusHandler,
            ExampleDataProvider,
            ExampleDataProcessingEnvironment,
            ExampleExporter,
            unit_test=True,
        )

        assert ts._process_proc_batch(proc_batch_id) == success
        verify(ExampleDataProcessingEnvironment, times=1).process_batch(proc_batch_id)
        verify(logger_mock, times=2 if proc_env_success else 1).info(ANY)
        verify(logger_mock, times=0 if proc_env_success else 1).error(ANY)
