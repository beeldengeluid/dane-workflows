from mockito import unstub
import pytest
from dane_workflows.data_processing import ExampleDataProcessingEnvironment
from dane_workflows.status import ExampleStatusHandler, ProcessingStatus
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


@pytest.mark.parametrize(
    ("proc_batch_id"),
    [(0)],
)
def test_process_batch(config, proc_batch_id):
    try:
        status_handler = ExampleStatusHandler(config)
        dpe = ExampleDataProcessingEnvironment(config, status_handler)
        assert dpe is not None
    finally:
        unstub()


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
