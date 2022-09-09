import sys
import pytest
from mockito import when, verify, ANY
from dane_workflows.task_scheduler import TaskScheduler
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.data_processing import (
    ExampleDataProcessingEnvironment,
)
from dane_workflows.exporter import ExampleExporter
from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
)
from dane_workflows.status_monitor import ExampleStatusMonitor
from test_util import new_batch


@pytest.mark.parametrize(
    ("error", "success"),
    [
        (None, True),
        ("no_ts_config", False),
        ("no_ts_batch_size", False),
        ("no_ts_batch_limit", True),
        ("no_ts_monitor_freq", True),
    ],
)
def test_validate_config(config, error, success):
    if error == "no_ts_config":
        del config["TASK_SCHEDULER"]
    elif error == "no_ts_batch_size":
        del config["TASK_SCHEDULER"]["BATCH_SIZE"]
    elif error == "no_ts_batch_limit":
        del config["TASK_SCHEDULER"]["BATCH_LIMIT"]

    with when(sys).exit().thenReturn():
        TaskScheduler(
            config,
            ExampleStatusHandler,
            ExampleDataProvider,
            ExampleDataProcessingEnvironment,
            ExampleExporter,
            unit_test=True,
        )
        verify(sys, times=0 if success else 1).exit()


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
    with when(  # mock success/failure by returning empty status_rows or ones with proper status_rows
        ExampleDataProcessingEnvironment
    ).register_batch(
        proc_batch_id, proc_batch
    ).thenReturn(
        new_batch(0, ProcessingStatus.BATCH_REGISTERED) if proc_env_success else None
    ):
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


@pytest.mark.parametrize(
    ("proc_batch_id", "proc_env_success", "success"),
    [
        (0, True, True),
        (0, False, False),
    ],
)
def test_process_proc_batch(config, proc_batch_id, proc_env_success, success):
    with when(  # mock success/failure by returning empty status_rows or ones with proper status_rows
        ExampleDataProcessingEnvironment
    ).process_batch(
        proc_batch_id
    ).thenReturn(
        new_batch(0, ProcessingStatus.PROCESSING) if proc_env_success else None
    ):
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


@pytest.mark.parametrize(
    ("batch_limit", "proc_batch_id", "sys_exit"),
    [
        (-1, 0, 0),
        (-1, 5, 0),
        (0, 0, 1),
        (0, 5, 1),
        (1, 0, 0),
        (1, 1, 1),
        (5, 0, 0),
        (5, 4, 0),
        (5, 5, 1),
        (5, 6, 1),
    ],
)
def test_check_batch_limit(config, proc_batch_id, batch_limit, sys_exit):

    ts = TaskScheduler(
        config,
        ExampleStatusHandler,
        ExampleDataProvider,
        ExampleDataProcessingEnvironment,
        ExampleExporter,
        ExampleStatusMonitor,
        unit_test=True,
    )
    ts.BATCH_LIMIT = batch_limit
    with when(sys).exit().thenReturn():
        ts._check_batch_limit(proc_batch_id)
        verify(sys, times=sys_exit).exit()
