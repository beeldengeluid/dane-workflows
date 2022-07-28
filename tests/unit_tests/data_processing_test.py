from mockito import unstub, when, ANY, verify, spy2
import pytest
import sys
import dane_workflows.util.base_util 
from dane_workflows.data_processing import ExampleDataProcessingEnvironment
from dane_workflows.status import ExampleStatusHandler, ProcessingStatus, ErrorCode
from test_util import new_batch, LoggerMock


@pytest.mark.parametrize(
    ("proc_batch_id", "proc_batch", "register_success", "status_persisted"),
    [
        (0, new_batch(0, ProcessingStatus.NEW, None, 5), True, True),
        (0, new_batch(0, ProcessingStatus.NEW, None, 5), False, True),
        (0, new_batch(0, ProcessingStatus.NEW, None, 5), True, False)
    ],
)
def test_register_batch(config, proc_batch_id, proc_batch, register_success, status_persisted):
    logger_mock = LoggerMock()
    status_handler = ExampleStatusHandler(config)
    dpe = ExampleDataProcessingEnvironment(config, status_handler)
    with when(dane_workflows.util.base_util).init_logger(config).thenReturn(
        logger_mock
    ), when(  # mock success/failure by returning empty status_rows or ones with proper status_rows
        dpe
    )._register_batch(
        proc_batch_id, proc_batch
    ).thenReturn(
        new_batch(0, ProcessingStatus.BATCH_REGISTERED, None, 5) if register_success else None
    ), when(
        status_handler
    ).persist(
        ANY
    ).thenReturn(
        status_persisted
    ), when(  # mock, so it never actually quits the program
        sys
    ).exit(
    ).thenReturn():
        spy2(dpe._set_register_batch_failed)
        spy2(status_handler.persist_or_die)
        status_rows = dpe.register_batch(proc_batch_id, proc_batch)
        if register_success:
            for row in status_rows:
                assert row.status == ProcessingStatus.BATCH_REGISTERED
        verify(dpe, times=0 if register_success else 1)._set_register_batch_failed(ANY, proc_batch_id)
        verify(status_handler, times=1).persist_or_die(ANY)
        verify(status_handler, times=1).persist(ANY)


@pytest.mark.parametrize(  # TODO add tests for the unhappy flow
    ("proc_batch_id", "proc_env_error", "status_persisted"),
    [(0, False, True), (0, True, True), (0, True, False)],
)
def test_process_batch(config, proc_batch_id, proc_env_error, status_persisted):
    logger_mock = LoggerMock()
    status_handler = ExampleStatusHandler(config)
    dpe = ExampleDataProcessingEnvironment(config, status_handler)
    with when(dane_workflows.util.base_util).init_logger(config).thenReturn(
        logger_mock
    ), when(dpe)._set_by_processing_response(
        proc_batch_id, ANY
    ).thenReturn(
        new_batch(0, ProcessingStatus.ERROR, ErrorCode.BATCH_PROCESSING_NOT_STARTED)
        if proc_env_error
        else new_batch(0, ProcessingStatus.PROCESSING)
    ), when(
        status_handler
    ).persist(
        ANY
    ).thenReturn(
        status_persisted
    ), when(  # mock, so it never actually quits the program
        sys
    ).exit(
    ).thenReturn():
        spy2(dpe._set_register_batch_failed)
        spy2(status_handler.persist_or_die)
        status_rows = dpe.process_batch(proc_batch_id)
        for row in status_rows:
            if proc_env_error:
                assert row.status == ProcessingStatus.ERROR
                assert row.proc_error_code == ErrorCode.BATCH_PROCESSING_NOT_STARTED
            else:
                assert row.status == ProcessingStatus.PROCESSING

        verify(dpe, times=1)._set_by_processing_response(
            proc_batch_id, ANY
        )
        verify(status_handler, times=1).persist_or_die(ANY)
        verify(status_handler, times=1).persist(ANY)


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
