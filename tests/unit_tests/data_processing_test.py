from mockito import unstub, when, ANY, verify
import pytest
from dane_workflows.data_processing import ExampleDataProcessingEnvironment
from dane_workflows.status import ExampleStatusHandler, ProcessingStatus, ErrorCode
from test_util import new_batch


@pytest.mark.parametrize(
    ("proc_batch_id", "proc_batch"),
    [(0, new_batch(0, ProcessingStatus.NEW))],
)
def test_register_batch(config, proc_batch_id, proc_batch):
    try:
        status_handler = ExampleStatusHandler(config)
        dpe = ExampleDataProcessingEnvironment(config, status_handler)
        status_rows = dpe.register_batch(proc_batch_id, proc_batch)
        for row in status_rows:
            assert row.status == ProcessingStatus.BATCH_REGISTERED

    finally:
        unstub()


@pytest.mark.parametrize(  # TODO add tests for the unhappy flow
    ("proc_batch_id", "proc_env_error"),
    [(0, False), (0, True)],
)
def test_process_batch(config, proc_batch_id, proc_env_error):
    with when(ExampleDataProcessingEnvironment).set_by_processing_response(
        proc_batch_id, ANY
    ).thenReturn(
        new_batch(0, ProcessingStatus.ERROR, ErrorCode.BATCH_PROCESSING_NOT_STARTED)
        if proc_env_error
        else new_batch(0, ProcessingStatus.PROCESSING)
    ):
        status_handler = ExampleStatusHandler(config)
        dpe = ExampleDataProcessingEnvironment(config, status_handler)
        status_rows = dpe.process_batch(proc_batch_id)
        for row in status_rows:
            if proc_env_error:
                assert row.status == ProcessingStatus.ERROR
                assert row.proc_error_code == ErrorCode.BATCH_PROCESSING_NOT_STARTED
            else:
                assert row.status == ProcessingStatus.PROCESSING

        verify(ExampleDataProcessingEnvironment, times=1).set_by_processing_response(
            proc_batch_id, ANY
        )


@pytest.mark.parametrize(
    ("proc_batch_id"),
    [(0)],
)
def test_monitor_batch(config, proc_batch_id):
    try:
        status_handler = ExampleStatusHandler(config)
        dpe = ExampleDataProcessingEnvironment(config, status_handler)
        assert dpe is not None
    finally:
        unstub()
