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


@pytest.mark.parametrize(
    ("is_unit_test", "source_batch_recovered", "status_rows"),
    [
        (True, False, None),
        (True, True, None),
        (True, False, ["dummy-status-row"]),
        (False, False, None),
        (False, True, None),
        (False, False, ["dummy-status-row"]),
    ],
)
def test___init__(config, is_unit_test, source_batch_recovered, status_rows):
    logger_mock = LoggerMock()
    with when(TaskScheduler)._validate_config().thenReturn(True), when(
        dane_workflows.util.base_util
    ).init_logger(config).thenReturn(logger_mock), when(
        ExampleStatusHandler
    ).recover().thenReturn(
        (source_batch_recovered, "dummy-proc-batch-id")
    ), when(
        ExampleDataProvider
    ).fetch_source_batch_data(
        0
    ).thenReturn(
        status_rows
    ), when(
        ExampleStatusHandler
    ).set_current_source_batch(
        status_rows
    ).thenReturn():

        if is_unit_test or source_batch_recovered or status_rows is not None:
            task_scheduler = TaskScheduler(
                config,
                ExampleStatusHandler,
                ExampleDataProvider,
                ExampleDataProcessingEnvironment,
                ExampleExporter,
                is_unit_test,
            )
            assert task_scheduler.BATCH_SIZE == config["TASK_SCHEDULER"]["BATCH_SIZE"]
            assert (
                task_scheduler.BATCH_PREFIX == config["TASK_SCHEDULER"]["BATCH_PREFIX"]
            )

            assert isinstance(task_scheduler.status_handler, ExampleStatusHandler)
            assert isinstance(task_scheduler.data_provider, ExampleDataProvider)
            assert isinstance(task_scheduler.exporter, ExampleExporter)

            verify(TaskScheduler, times=1)._validate_config()
            verify(dane_workflows.util.base_util, times=1).init_logger(config)
            verify(ExampleStatusHandler, times=0 if is_unit_test else 1).recover()
            verify(
                ExampleDataProvider,
                times=1 if (not is_unit_test and not source_batch_recovered) else 0,
            ).fetch_source_batch_data(0)
            verify(
                ExampleStatusHandler,
                times=1
                if (
                    not is_unit_test
                    and not source_batch_recovered
                    and status_rows is not None
                )
                else 0,
            ).set_current_source_batch(status_rows)

        else:
            with pytest.raises(SystemExit):
                task_scheduler = TaskScheduler(
                    config,
                    ExampleStatusHandler,
                    ExampleDataProvider,
                    ExampleDataProcessingEnvironment,
                    ExampleExporter,
                    is_unit_test,
                )


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


@pytest.mark.parametrize("batch_id", [0, 1, 10, 100, 5000])
def test__to_dane_batch_name(config, batch_id):
    task_scheduler = TaskScheduler(
        config,
        ExampleStatusHandler,
        ExampleDataProvider,
        ExampleDataProcessingEnvironment,
        ExampleExporter,
        True,
    )

    dane_batch_name = task_scheduler._to_dane_batch_name(batch_id)

    assert dane_batch_name == task_scheduler.BATCH_PREFIX + "__" + str(batch_id)


@pytest.mark.parametrize(
    (
        "proc_batch_ids",
        "list_status_rows_dp",
        "list_status_rows_dpe",
        "list_proc_resp_success",
        "list_status_rows_monitor",
        "list_processing_results",
    ),
    [
        # one batch
        ([-1], [None], [None], [False], [None], [None]),
        ([-1], [["dummy-dp"]], [None], [False], [None], [None]),
        ([-1], [["dummy-dp"]], [["dummy-dpe"]], [False], [None], [None]),
        ([-1], [["dummy-dp"]], [["dummy-dpe"]], [True], [None], [None]),
        ([-1], [["dummy-dp"]], [["dummy-dpe"]], [True], [["dummy-monitor"]], [None]),
        (
            [-1],
            [["dummy-dp"]],
            [["dummy-dpe"]],
            [True],
            [["dummy-monitor"]],
            [["dummy-proc"]],
        ),
        ([0], [None], [None], [False], [None], [None]),
        ([0], [["dummy-dp"]], [None], [False], [None], [None]),
        ([0], [["dummy-dp"]], [["dummy-dpe"]], [False], [None], [None]),
        ([0], [["dummy-dp"]], [["dummy-dpe"]], [True], [None], [None]),
        ([0], [["dummy-dp"]], [["dummy-dpe"]], [True], [["dummy-monitor"]], [None]),
        (
            [0],
            [["dummy-dp"]],
            [["dummy-dpe"]],
            [True],
            [["dummy-monitor"]],
            [["dummy-proc"]],
        ),
        # two batches
        (
            [0, 1],
            [["dummy-dp"], None],
            [["dummy-dpe"], None],
            [True, False],
            [["dummy-monitor"], None],
            [["dummy-proc"], None],
        ),
        (
            [0, 1],
            [["dummy-dp"], ["dummy-dp-2"]],
            [["dummy-dpe"], None],
            [True, False],
            [["dummy-monitor"], None],
            [["dummy-proc"], None],
        ),
        (
            [0, 1],
            [["dummy-dp"], ["dummy-dp-2"]],
            [["dummy-dpe"], ["dummy-dpe-2"]],
            [True, False],
            [["dummy-monitor"], None],
            [["dummy-proc"], None],
        ),
        (
            [0, 1],
            [["dummy-dp"], ["dummy-dp-2"]],
            [["dummy-dpe"], ["dummy-dpe-2"]],
            [True, True],
            [["dummy-monitor"], None],
            [["dummy-proc"], None],
        ),
        (
            [0, 1],
            [["dummy-dp"], ["dummy-dp-2"]],
            [["dummy-dpe"], ["dummy-dpe-2"]],
            [True, True],
            [["dummy-monitor"], ["dummy-monitor-2"]],
            [["dummy-proc"], None],
        ),
        (
            [0, 1],
            [["dummy-dp"], ["dummy-dp-2"]],
            [["dummy-dpe"], ["dummy-dpe-2"]],
            [True, True],
            [["dummy-monitor"], ["dummy-monitor-2"]],
            [["dummy-proc"], ["dummy-proc-2"]],
        ),
    ],
)
def test_run(
    config,
    proc_batch_ids,
    list_status_rows_dp,
    list_status_rows_dpe,
    list_proc_resp_success,
    list_status_rows_monitor,
    list_processing_results,
):

    task_scheduler = TaskScheduler(
        config,
        ExampleStatusHandler,
        ExampleDataProvider,
        ExampleDataProcessingEnvironment,
        ExampleExporter,
        True,
    )

    initial_proc_batch_id = proc_batch_ids[0]
    if proc_batch_ids[0] == -1:
        proc_batch_ids[0] = 0  # set to the value that should be used

    try:
        when(ExampleStatusHandler).get_last_proc_batch_id().thenReturn(
            initial_proc_batch_id
        )

        # the remaining functions are repeated in the loop
        list_proc_resp = [
            ProcEnvResponse(
                success=proc_resp_success, status_code=0, status_text="dummy-text"
            )
            for proc_resp_success in list_proc_resp_success
        ]

        for i, proc_batch_id in enumerate(proc_batch_ids):
            when(ExampleDataProvider).get_next_batch(proc_batch_id, ANY).thenReturn(
                list_status_rows_dp[i]
            )
            when(ExampleDataProcessingEnvironment).register_batch(
                proc_batch_id, list_status_rows_dp[i]
            ).thenReturn(list_status_rows_dpe[i])
            when(ExampleStatusHandler).persist(list_status_rows_dpe[i])
            when(ExampleDataProcessingEnvironment).process_batch(
                proc_batch_id
            ).thenReturn(list_proc_resp[i])
            when(TaskScheduler)._update_status(
                list_status_rows_dpe[i],
                status=ProcessingStatus.PROCESSING,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
            )
            when(ExampleDataProcessingEnvironment).monitor_batch(
                proc_batch_id
            ).thenReturn(list_status_rows_monitor[i])
            when(ExampleStatusHandler).persist(list_status_rows_monitor[i])
            when(ExampleDataProcessingEnvironment).fetch_results_of_batch(
                proc_batch_id
            ).thenReturn(list_processing_results[i])
            when(ExampleExporter).export_results(list_processing_results[i])
            when(TaskScheduler)._update_status(
                list_status_rows_dpe[i],
                status=ProcessingStatus.ERROR,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
                proc_error_code=ErrorCode.BATCH_PROCESSING_NOT_STARTED,
            )
            when(TaskScheduler)._update_status(
                list_status_rows_dp[i],
                status=ProcessingStatus.ERROR,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
                proc_error_code=ErrorCode.BATCH_REGISTER_FAILED,
            )

        # data provider is called one last time if processing was successful, to see if there is any more to process
        when(ExampleDataProvider).get_next_batch(
            proc_batch_ids[-1] + 1, ANY
        ).thenReturn(None)

        task_scheduler.run()

        # now verify everything was called as many times as it should be
        for i, proc_batch_id in enumerate(proc_batch_ids):
            verify(ExampleDataProvider, times=1).get_next_batch(proc_batch_id, ANY)
            verify(
                ExampleDataProcessingEnvironment,
                times=1 if list_status_rows_dp[i] else 0,
            ).register_batch(proc_batch_id, list_status_rows_dp[i])
            verify(
                ExampleStatusHandler, times=1 if list_status_rows_dpe[i] else 0
            ).persist(list_status_rows_dpe[i])
            verify(
                ExampleDataProcessingEnvironment,
                times=1 if list_status_rows_dpe[i] else 0,
            ).process_batch(proc_batch_id)
            verify(
                TaskScheduler,
                times=1 if list_proc_resp_success[i] and list_status_rows_dpe[i] else 0,
            )._update_status(
                list_status_rows_dpe[i],
                status=ProcessingStatus.PROCESSING,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
            )
            verify(
                ExampleDataProcessingEnvironment,
                times=1 if list_proc_resp_success[i] and list_status_rows_dpe[i] else 0,
            ).monitor_batch(proc_batch_id)
            verify(
                ExampleStatusHandler, times=1 if list_status_rows_monitor[i] else 0
            ).persist(list_status_rows_monitor[i])
            verify(
                ExampleDataProcessingEnvironment,
                times=1 if list_status_rows_monitor[i] else 0,
            ).fetch_results_of_batch(proc_batch_id)
            verify(
                ExampleExporter, times=1 if list_processing_results[i] else 0
            ).export_results(list_processing_results[i])
            # get 'processing not started' error if batch was registered but processing response is not success
            verify(
                TaskScheduler,
                times=1
                if not list_proc_resp_success[i] and list_status_rows_dpe[i]
                else 0,
            )._update_status(
                list_status_rows_dpe[i],
                status=ProcessingStatus.ERROR,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
                proc_error_code=ErrorCode.BATCH_PROCESSING_NOT_STARTED,
            )
            # get 'register failed' error if data provider returned data but registering dind't
            verify(
                TaskScheduler,
                times=1
                if list_status_rows_dp[i] and not list_status_rows_dpe[i]
                else 0,
            )._update_status(
                list_status_rows_dp[i],
                status=ProcessingStatus.ERROR,
                proc_batch_id=proc_batch_id,
                proc_status_msg=ANY,
                proc_error_code=ErrorCode.BATCH_REGISTER_FAILED,
            )

            # data provider is called one last time if processing was successful, to see if there is any more to process
            verify(
                ExampleDataProvider, times=1 if list_processing_results[-1] else 0
            ).get_next_batch(proc_batch_ids[-1] + 1, ANY)

    finally:
        unstub()
