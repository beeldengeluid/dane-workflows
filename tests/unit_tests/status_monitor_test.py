import pytest
from mockito import when, verify

from dane_workflows.status_monitor import StatusMonitor
from dane_workflows.data_provider import ExampleDataProvider
from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
    ErrorCode,
)


def test_check_status(config):
    status_handler = ExampleStatusHandler(config)
    status_monitor = StatusMonitor(
        config, status_handler, ExampleDataProvider(config, status_handler)
    )
    dummy_last_proc_batch_id = 1
    dummy_last_source_batch_id = 2

    dummy_error_code_counts_for_proc_batch = {
        ErrorCode.IMPOSSIBLE: 2,
        ErrorCode.BATCH_REGISTER_FAILED: 1,
    }

    dummy_status_counts_for_proc_batch = {
        ProcessingStatus.ERROR: 3,
        ProcessingStatus.NEW: 1,
        ProcessingStatus.FINISHED: 2,
    }
    dummy_error_code_counts_for_source_batch = {
        ErrorCode.IMPOSSIBLE: 1,
        ErrorCode.EXPORT_FAILED_SOURCE_DB_CONNECTION_FAILURE: 2,
        ErrorCode.EXPORT_FAILED_SOURCE_DOC_NOT_FOUND: 1,
    }

    dummy_status_counts_for_source_batch = {
        ProcessingStatus.ERROR: 4,
        ProcessingStatus.NEW: 5,
        ProcessingStatus.BATCH_ASSIGNED: 2,
    }
    with when(ExampleStatusHandler).get_last_proc_batch_id().thenReturn(
        dummy_last_proc_batch_id
    ), when(ExampleStatusHandler).get_last_source_batch_id().thenReturn(
        dummy_last_source_batch_id
    ), when(
        ExampleStatusHandler
    ).get_status_counts_for_proc_batch_id(
        dummy_last_proc_batch_id
    ).thenReturn(
        dummy_status_counts_for_proc_batch
    ), when(
        ExampleStatusHandler
    ).get_error_code_counts_for_proc_batch_id(
        dummy_last_proc_batch_id
    ).thenReturn(
        dummy_error_code_counts_for_proc_batch
    ), when(
        ExampleStatusHandler
    ).get_status_counts_for_source_batch_id(
        dummy_last_source_batch_id
    ).thenReturn(
        dummy_status_counts_for_source_batch
    ), when(
        ExampleStatusHandler
    ).get_error_code_counts_for_source_batch_id(
        dummy_last_source_batch_id
    ).thenReturn(
        dummy_error_code_counts_for_source_batch
    ):

        status_info = status_monitor.check_status()

        assert status_info["Date started"] == status_monitor.date_started.strftime(
            "%Y-%m-%d"
        )
        assert status_info["Last batch processed"] == dummy_last_proc_batch_id
        assert status_info["Last source batch retrieved"] == dummy_last_source_batch_id

        for key in dummy_status_counts_for_proc_batch:
            assert f"{key}: {dummy_status_counts_for_proc_batch[key]}" in str(
                status_info
            )
        for key in dummy_error_code_counts_for_proc_batch:
            assert f"{key}: {dummy_error_code_counts_for_proc_batch[key]}" in str(
                status_info
            )
        for key in dummy_status_counts_for_source_batch:
            assert f"{key}: {dummy_status_counts_for_source_batch[key]}" in str(
                status_info
            )
        for key in dummy_error_code_counts_for_source_batch:
            assert f"{key}: {dummy_error_code_counts_for_source_batch[key]}" in str(
                status_info
            )

        verify(ExampleStatusHandler, times=1).get_last_proc_batch_id()
        verify(ExampleStatusHandler, times=1).get_last_source_batch_id()
        verify(ExampleStatusHandler, times=1).get_status_counts_for_proc_batch_id(
            dummy_last_proc_batch_id
        )
        verify(ExampleStatusHandler, times=1).get_error_code_counts_for_proc_batch_id(
            dummy_last_proc_batch_id
        )
        verify(ExampleStatusHandler, times=1).get_status_counts_for_source_batch_id(
            dummy_last_source_batch_id
        )
        verify(ExampleStatusHandler, times=1).get_error_code_counts_for_source_batch_id(
            dummy_last_source_batch_id
        )


@pytest.mark.parametrize("include_extra_info", [False, True])
def test_get_detailed_status_report(config, include_extra_info):
    status_handler = ExampleStatusHandler(config)
    status_monitor = StatusMonitor(
        config, status_handler, ExampleDataProvider(config, status_handler)
    )

    dummy_incomplete = ["dummy-incomplete-1", "dummy-incomplete-2"]
    dummy_complete = [
        "dummy-complete-1",
        "dummy-complete-2",
        "dummy-complete-3",
        "dummy-complete-4",
    ]
    dummy_source_batch_id = "dummy-source-batch-id"
    dummy_semantic_source_batch_id = "dummy-semantic-source-batch-id"
    dummy_status_counts = {
        ProcessingStatus.NEW: 1,
        ProcessingStatus.ERROR: 3,
        ProcessingStatus.FINISHED: 5,
    }
    dummy_error_code_counts = {
        ErrorCode.IMPOSSIBLE: 1,
        ErrorCode.EXPORT_FAILED_SOURCE_DB_CONNECTION_FAILURE: 1,
        ErrorCode.BATCH_ASSIGN_FAILED: 1,
    }
    dummy_status_counts_per_extra_info = {
        "dummy-genre-1": {ProcessingStatus.NEW: 1},
        "dummy-genre-2": {ProcessingStatus.ERROR: 2, ProcessingStatus.FINISHED: 3},
        "dummy-genre-3": {ProcessingStatus.ERROR: 1, ProcessingStatus.FINISHED: 2},
    }

    with when(
        ExampleStatusHandler
    ).get_completed_semantic_source_batch_ids().thenReturn(
        (dummy_complete, dummy_incomplete)
    ), when(
        ExampleStatusHandler
    ).get_last_source_batch_id().thenReturn(
        dummy_source_batch_id
    ), when(
        ExampleDataProvider
    )._to_semantic_source_batch_id(
        dummy_source_batch_id
    ).thenReturn(
        dummy_semantic_source_batch_id
    ), when(
        ExampleStatusHandler
    ).get_status_counts().thenReturn(
        dummy_status_counts
    ), when(
        ExampleStatusHandler
    ).get_error_code_counts().thenReturn(
        dummy_error_code_counts
    ), when(
        ExampleStatusHandler
    ).get_status_counts_per_extra_info_value().thenReturn(
        dummy_status_counts_per_extra_info
    ):

        status_report = status_monitor.get_detailed_status_report(include_extra_info)

        assert "Completed semantic source batch IDs" in status_report
        assert status_report["Completed semantic source batch IDs"] == dummy_complete
        assert "Uncompleted semantic source batch IDs" in status_report
        assert (
            status_report["Uncompleted semantic source batch IDs"] == dummy_incomplete
        )
        assert "Current semantic source batch ID" in status_report
        assert (
            status_report["Current semantic source batch ID"]
            == dummy_semantic_source_batch_id
        )
        assert "Status overview" in status_report
        assert status_report["Status overview"] == dummy_status_counts
        assert "Error overview" in status_report
        assert status_report["Error overview"] == dummy_error_code_counts

        if include_extra_info:
            assert "Status overview per extra info" in status_report
            assert (
                status_report["Status overview per extra info"]
                == dummy_status_counts_per_extra_info
            )
        else:
            assert "Status overview per extra info" not in status_report

        verify(ExampleStatusHandler, times=1).get_completed_semantic_source_batch_ids()
        verify(ExampleStatusHandler, times=1).get_last_source_batch_id()
        verify(ExampleDataProvider, times=1)._to_semantic_source_batch_id(
            dummy_source_batch_id
        )
        verify(ExampleStatusHandler, times=1).get_status_counts()
        verify(ExampleStatusHandler, times=1).get_error_code_counts()
        verify(
            ExampleStatusHandler, times=1 if include_extra_info else 0
        ).get_status_counts_per_extra_info_value()
