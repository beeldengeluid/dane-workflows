import pytest
from mockito import unstub, when, verify, ARGS
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
)
from test_util import new_batch


@pytest.mark.parametrize(
    ("status_rows", "proc_batch_id", "batch_size", "success"),
    [
        (new_batch(0, ProcessingStatus.NEW), 0, 10, True),
        (new_batch(0, ProcessingStatus.NEW), 0, 99, True),
        (new_batch(0, ProcessingStatus.NEW), 0, 100, True),
        (new_batch(0, ProcessingStatus.NEW), 0, 1000000, False),
        (new_batch(0, ProcessingStatus.FINISHED), 0, 10, True),
    ],
)
def test_get_next_batch(config, status_rows, proc_batch_id, batch_size, success):
    try:
        prev_batch_done = status_rows[0].status == ProcessingStatus.FINISHED

        # prepare a status_handler with some data
        status_handler = ExampleStatusHandler(config)
        status_handler.set_current_source_batch(status_rows)

        # don't persist to file during unit test
        when(status_handler).persist(*ARGS).thenReturn(True)

        #
        when(ExampleDataProvider).fetch_source_batch_data(1).thenReturn(
            new_batch(1, ProcessingStatus.NEW)
        )

        # TODO verify calls to get_next_batch to test recursion cases

        dp = ExampleDataProvider(config, status_handler)

        # don't simulate the recursive call directly
        resulting_rows = dp.get_next_batch(proc_batch_id, batch_size, False)
        assert (len(resulting_rows) == batch_size) is success
        for row in resulting_rows:
            assert row.status == ProcessingStatus.BATCH_ASSIGNED
            assert row.proc_batch_id == proc_batch_id

        # called recursively, so 2 times
        # verify(status_handler, times=2 if prev_batch_done else 1).persist(
        #     *ARGS
        # )

        verify(
            ExampleDataProvider, times=1 if prev_batch_done else 0
        ).fetch_source_batch_data(1)

        # verify(dp, times=2).get_next_batch(proc_batch_id, batch_size, True)
    finally:
        unstub()
