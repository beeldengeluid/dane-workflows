import os
import pytest
from mockito import when, verify, spy2, ANY, unstub

import dane_workflows.util.base_util
from dane_workflows.task_scheduler import TaskScheduler
import dane_workflows.data_provider
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.data_processing import (
    ExampleDataProcessingEnvironment,
    ProcEnvResponse,
)
from dane_workflows.exporter import ExampleExporter
from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
    ErrorCode,
)


class LoggerMock(object):
    def __enter__(self):
        pass

    def __exit__(self, one, two, three):
        pass

    def info(self, info_string):
        pass

    def error(self, info_string):
        pass


# @pytest.mark.parametrize(
#     ("is_unit_test", "source_batch_recovered", "status_rows"),
#     [
#         (True, False, None),
#         (True, True, None),
#         (True, False, ["dummy-status-row"]),
#         (False, False, None),
#         (False, True, None),
#         (False, False, ["dummy-status-row"]),
#     ],
# )
# def test___init__(config, is_unit_test, source_batch_recovered, status_rows):
#     logger_mock = LoggerMock()
#     with when(TaskScheduler)._validate_config().thenReturn(True), when(
#         dane_workflows.util.base_util
#     ).init_logger(config).thenReturn(logger_mock), when(
#         ExampleStatusHandler
#     ).recover().thenReturn(
#         (source_batch_recovered, "dummy-proc-batch-id")
#     ), when(
#         ExampleDataProvider
#     ).fetch_source_batch_data(
#         0
#     ).thenReturn(
#         status_rows
#     ), when(
#         ExampleStatusHandler
#     ).set_current_source_batch(
#         status_rows
#     ).thenReturn():

#         if is_unit_test or source_batch_recovered or status_rows is not None:
#             task_scheduler = TaskScheduler(
#                 config,
#                 ExampleStatusHandler,
#                 ExampleDataProvider,
#                 ExampleDataProcessingEnvironment,
#                 ExampleExporter,
#                 is_unit_test,
#             )
#             assert task_scheduler.BATCH_SIZE == config["TASK_SCHEDULER"]["BATCH_SIZE"]
#             assert (
#                 task_scheduler.BATCH_PREFIX == config["TASK_SCHEDULER"]["BATCH_PREFIX"]
#             )

#             assert isinstance(task_scheduler.status_handler, ExampleStatusHandler)
#             assert isinstance(task_scheduler.data_provider, ExampleDataProvider)
#             assert isinstance(task_scheduler.exporter, ExampleExporter)

#             verify(TaskScheduler, times=1)._validate_config()
#             verify(dane_workflows.util.base_util, times=1).init_logger(config)
#             verify(ExampleStatusHandler, times=0 if is_unit_test else 1).recover()
#             verify(
#                 ExampleDataProvider,
#                 times=1 if (not is_unit_test and not source_batch_recovered) else 0,
#             ).fetch_source_batch_data(0)
#             verify(
#                 ExampleStatusHandler,
#                 times=1
#                 if (
#                     not is_unit_test
#                     and not source_batch_recovered
#                     and status_rows is not None
#                 )
#                 else 0,
#             ).set_current_source_batch(status_rows)

#         else:
#             with pytest.raises(SystemExit):
#                 task_scheduler = TaskScheduler(
#                     config,
#                     ExampleStatusHandler,
#                     ExampleDataProvider,
#                     ExampleDataProcessingEnvironment,
#                     ExampleExporter,
#                     is_unit_test,
#                 )


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
                True,
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
                True,
            )

            verify(dane_workflows.util.base_util, times=1).check_log_level(ANY)
            verify(dane_workflows.util.base_util, times=4).check_setting(ANY, ANY)
            verify(dane_workflows.util.base_util, times=1).validate_parent_dirs(ANY)
        finally:
            unstub()
