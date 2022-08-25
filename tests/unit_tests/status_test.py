import time
import os
from os import sep
import pytest

from mockito import unstub, when, verify, spy2, ANY
from test_util import new_batch
from datetime import datetime
from dane_workflows.status import (
    ExampleStatusHandler,
    SQLiteStatusHandler,
    StatusRow,
    ProcessingStatus,
    ErrorCode,
)


def test_get_current_source_batch(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_set_current_source_batch(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_get_sb_status_rows_of_type(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_get_status_rows_of_proc_batch(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_get_cur_source_batch_id(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_get_last_proc_batch_id(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


def test_update_status_rows(config):
    try:
        status_handler = ExampleStatusHandler(config)
        assert status_handler is not None
    finally:
        unstub()


@pytest.mark.parametrize(
    ("status_rows", "success"),
    [
        (new_batch(0, ProcessingStatus.NEW, None, 5), True),  # only this should succeed
        (None, False),
        ({}, False),
        ("", False),
    ],
)
def test_persist__input_types(config, status_rows, success):
    try:
        status_handler = ExampleStatusHandler(config)
        spy2(status_handler._persist)
        spy2(status_handler._update_status_rows_modification_date)
        spy2(status_handler._recover_source_batch)
        assert status_handler.persist(status_rows) is success
        verify(status_handler, times=1 if success else 0)._persist(ANY)
        verify(
            status_handler, times=1 if success else 0
        )._update_status_rows_modification_date(ANY)
        verify(status_handler, times=1 if success else 0)._recover_source_batch()

    finally:
        unstub()


def test_persist__failed_source_batch_recovery(config):
    status_handler = ExampleStatusHandler(config)
    status_rows = new_batch(0, ProcessingStatus.NEW, None, 5)
    with when(status_handler)._recover_source_batch().thenReturn(False):
        spy2(status_handler._persist)
        spy2(status_handler._update_status_rows_modification_date)

        assert status_handler.persist(status_rows) is False
        verify(status_handler, times=1)._persist(ANY)
        verify(status_handler, times=1)._update_status_rows_modification_date(ANY)
        verify(status_handler, times=1)._recover_source_batch()


# test if the _update_status_rows_modification_date function
# assigns a fresh date_modified to each StatusRow
def test_update_status_rows_modification_date(config):
    try:
        status_handler = ExampleStatusHandler(config)

        # create some StatusRow objects, a default date_modified should be assigned
        # (see test__default_status_row_date_fields())
        status_rows = new_batch(0, ProcessingStatus.NEW, None, 5)
        old_row_date_modified = status_rows[0].date_modified
        time.sleep(1)  # sleep to ensure new date_modified will be later

        # after this function call, all rows should have a fresh date_modified
        updated_rows = status_handler._update_status_rows_modification_date(status_rows)
        for row in updated_rows:
            td = row.date_modified - old_row_date_modified
            assert td.total_seconds() <= 2
            assert td.total_seconds() > 0

    finally:
        unstub()


# tests if the StatusRow object is always supplied with date_modified and date_created
def test__default_status_row_date_fields(config):
    status_rows = new_batch(0, ProcessingStatus.NEW, None, 5)
    assert all(
        row.date_modified is not None and row.date_created is not None
        for row in status_rows
    )


""" --------------------- SQLLITE Status Handler Tests ------------------ """


@pytest.mark.parametrize(
    ("statuses", "proc_batch_ids", "expected_status_counts"),
    [
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 1, 1, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 4],
            {ProcessingStatus.NEW: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {ProcessingStatus.NEW: 1, ProcessingStatus.PROCESSING: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {ProcessingStatus.NEW: 1, ProcessingStatus.PROCESSING: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 1, 1],
            {ProcessingStatus.NEW: 2, ProcessingStatus.PROCESSING: 1},
        ),
    ],
)
def test_get_status_counts_for_proc_batch_id(
    config, statuses, proc_batch_ids, expected_status_counts
):
    try:
        config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

        # use a test folder to store the database so production database is not affected
        if os.getcwd().endswith("unit_tests"):
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
            }
        else:
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
            }

        status_handler = SQLiteStatusHandler(config)

        # clean up before the test
        status_handler._delete_all_rows()

        status_rows = [
            StatusRow(
                target_id=f"dummy-id_{i}",
                target_url=f"dummy-url_{i}",
                status=statuses[i],
                source_batch_id="dummy-source_batch_id",
                source_batch_name="dummy-source_batch_name",
                source_extra_info="something row-specific",
                proc_batch_id=proc_batch_ids[i],
                proc_id=None,
                proc_status_msg=None,
                proc_error_code=None,
            )
            for i in range(len(statuses))
        ]

        status_handler.persist(status_rows)

        status_codes = status_handler.get_status_counts_for_proc_batch_id(1)

        for status_code in status_codes:
            assert ProcessingStatus(status_code) in expected_status_counts
            assert (
                status_codes[status_code]
                == expected_status_counts[ProcessingStatus(status_code)]
            )

        # clean up after the test
        status_handler._delete_all_rows()

    finally:
        unstub()


@pytest.mark.parametrize(
    ("error_codes", "proc_batch_ids", "expected_error_code_counts"),
    [
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 1, 1, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 4],
            {ErrorCode.IMPOSSIBLE: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {ErrorCode.IMPOSSIBLE: 1, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {ErrorCode.IMPOSSIBLE: 1, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 1, 1],
            {ErrorCode.IMPOSSIBLE: 2, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
    ],
)
def test_get_error_code_counts_for_proc_batch_id(
    config, error_codes, proc_batch_ids, expected_error_code_counts
):
    try:
        config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

        # use a test folder to store the database so production database is not affected
        if os.getcwd().endswith("unit_tests"):
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
            }
        else:
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
            }

        status_handler = SQLiteStatusHandler(config)

        # clean up before the test
        status_handler._delete_all_rows()

        status_rows = [
            StatusRow(
                target_id=f"dummy-id_{i}",
                target_url=f"dummy-url_{i}",
                status=ProcessingStatus.NEW,
                source_batch_id="dummy-source_batch_id",
                source_batch_name="dummy-source_batch_name",
                source_extra_info="something row-specific",
                proc_batch_id=proc_batch_ids[i],
                proc_id=None,
                proc_status_msg=None,
                proc_error_code=error_codes[i],
            )
            for i in range(len(error_codes))
        ]

        status_handler.persist(status_rows)

        error_code_counts = status_handler.get_error_code_counts_for_proc_batch_id(1)

        for error_code in error_code_counts:
            assert ErrorCode(error_code) in expected_error_code_counts
            assert (
                error_code_counts[error_code]
                == expected_error_code_counts[ErrorCode(error_code)]
            )

        # clean up after the test
        status_handler._delete_all_rows()

    finally:
        unstub()


@pytest.mark.parametrize(
    ("statuses", "source_batch_ids", "expected_status_counts"),
    [
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 1, 1, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 4],
            {ProcessingStatus.NEW: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {ProcessingStatus.NEW: 1, ProcessingStatus.PROCESSING: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {ProcessingStatus.NEW: 1, ProcessingStatus.PROCESSING: 1},
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 1, 1],
            {ProcessingStatus.NEW: 2, ProcessingStatus.PROCESSING: 1},
        ),
    ],
)
def test_get_status_counts_for_source_batch_id(
    config, statuses, source_batch_ids, expected_status_counts
):
    try:
        config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

        # use a test folder to store the database so production database is not affected
        if os.getcwd().endswith("unit_tests"):
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
            }
        else:
            config["STATUS_HANDLER"]["CONFIG"] = {
                "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
            }

        status_handler = SQLiteStatusHandler(config)

        # clean up before the test
        status_handler._delete_all_rows()

        status_rows = [
            StatusRow(
                target_id=f"dummy-id_{i}",
                target_url=f"dummy-url_{i}",
                status=statuses[i],
                source_batch_id=source_batch_ids[i],
                source_batch_name="dummy-source_batch_name",
                source_extra_info="something row-specific",
                proc_batch_id="dummy-proc_batch_id",
                proc_id=None,
                proc_status_msg=None,
                proc_error_code=None,
            )
            for i in range(len(statuses))
        ]

        status_handler.persist(status_rows)

        status_counts = status_handler.get_status_counts_for_source_batch_id(1)

        for status_code in status_counts:
            assert ProcessingStatus(status_code) in expected_status_counts
            assert (
                status_counts[status_code]
                == expected_status_counts[ProcessingStatus(status_code)]
            )

        # clean up after the test
        status_handler._delete_all_rows()

    finally:
        unstub()


@pytest.mark.parametrize(
    ("error_codes", "source_batch_ids", "expected_error_code_counts"),
    [
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 1, 1, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 4],
            {ErrorCode.IMPOSSIBLE: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {ErrorCode.IMPOSSIBLE: 1, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {ErrorCode.IMPOSSIBLE: 1, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 1, 1],
            {ErrorCode.IMPOSSIBLE: 2, ErrorCode.BATCH_REGISTER_FAILED: 1},
        ),
    ],
)
def test_get_error_code_counts_for_source_batch_id(
    config, error_codes, source_batch_ids, expected_error_code_counts
):
    config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

    # use a test folder to store the database so production database is not affected
    if os.getcwd().endswith("unit_tests"):
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
        }
    else:
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
        }

    status_handler = SQLiteStatusHandler(config)

    # clean up before the test
    status_handler._delete_all_rows()

    status_rows = [
        StatusRow(
            target_id="dummy-id",
            target_url=f"dummy-url_{i}",
            status=ProcessingStatus.NEW,
            source_batch_id=source_batch_ids[i],
            source_batch_name="dummy-source_batch_name",
            source_extra_info="something row-specific",
            proc_batch_id="dummy-proc_batch_id",
            proc_id=None,
            proc_status_msg=None,
            proc_error_code=error_codes[i],
        )
        for i in range(len(error_codes))
    ]

    status_handler.persist(status_rows)

    error_code_counts = status_handler.get_error_code_counts_for_source_batch_id(1)

    for error_code in error_code_counts:
        assert ErrorCode(error_code) in expected_error_code_counts
        assert (
            error_code_counts[error_code]
            == expected_error_code_counts[ErrorCode(error_code)]
        )

    # clean up after the test
    status_handler._delete_all_rows()


@pytest.mark.parametrize(
    (
        "statuses",
        "semantic_source_batch_ids",
        "expected_complete",
        "expected_incomplete",
    ),
    [
        ([ProcessingStatus.NEW], [0], [], ["0"]),
        ([ProcessingStatus.FINISHED], [0], ["0"], []),
        ([ProcessingStatus.ERROR], [0], ["0"], []),
        ([ProcessingStatus.NEW, ProcessingStatus.PROCESSING], [0, 0], [], ["0"]),
        ([ProcessingStatus.FINISHED, ProcessingStatus.ERROR], [0, 0], ["0"], []),
        ([ProcessingStatus.ERROR, ProcessingStatus.FINISHED], [0, 0], ["0"], []),
        ([ProcessingStatus.NEW, ProcessingStatus.PROCESSING], [0, 1], [], ["0", "1"]),
        ([ProcessingStatus.FINISHED, ProcessingStatus.ERROR], [0, 1], ["0", "1"], []),
        ([ProcessingStatus.ERROR, ProcessingStatus.FINISHED], [0, 1], ["0", "1"], []),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
                ProcessingStatus.ERROR,
                ProcessingStatus.FINISHED,
            ],
            [0, 0, 1, 1],
            ["1"],
            ["0"],
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.FINISHED,
                ProcessingStatus.ERROR,
                ProcessingStatus.PROCESSING,
            ],
            [0, 0, 1, 1],
            [],
            ["0", "1"],
        ),
        (
            [
                ProcessingStatus.ERROR,
                ProcessingStatus.FINISHED,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [0, 0, 1, 1],
            ["0"],
            ["1"],
        ),
    ],
)
def test_get_completed_semantic_source_batch_ids(
    config, statuses, semantic_source_batch_ids, expected_complete, expected_incomplete
):

    config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

    # use a test folder to store the database so production database is not affected
    if os.getcwd().endswith("unit_tests"):
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
        }
    else:
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
        }
    status_handler = SQLiteStatusHandler(config)

    # clean up before the test
    status_handler._delete_all_rows()

    status_rows = [
        StatusRow(
            target_id="dummy-id",
            target_url=f"dummy-url_{i}",
            status=statuses[i],
            source_batch_id="dummy-source-batch-id",
            source_batch_name=semantic_source_batch_ids[i],
            source_extra_info="something row-specific",
            proc_batch_id="dummy-proc_batch_id",
            proc_id=None,
            proc_status_msg=None,
            proc_error_code=None,
        )
        for i in range(len(statuses))
    ]

    status_handler.persist(status_rows)

    complete, incomplete = status_handler.get_completed_semantic_source_batch_ids()

    assert complete == expected_complete
    assert incomplete == expected_incomplete


def test_get_running_statuses_and_completed_statuses():
    running_statuses = ProcessingStatus.running_statuses()
    completed_statuses = ProcessingStatus.completed_statuses()

    assert running_statuses
    assert completed_statuses

    for (
        status
    ) in ProcessingStatus:  # each status should be in either running or completed

        assert (status in running_statuses or status in completed_statuses) and not (
            status in running_statuses and status in completed_statuses
        )


@pytest.mark.parametrize(
    ("statuses", "source_batch_ids", "expected_status_counts"),
    [
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 1, 1, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 4],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 3, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.NEW,
                ProcessingStatus.PROCESSING,
            ],
            [1, 2, 1, 1],
            {
                ProcessingStatus.NEW: 2,
                ProcessingStatus.ERROR: 1,
                ProcessingStatus.PROCESSING: 1,
            },
        ),
    ],
)
def test_get_status_counts(config, statuses, source_batch_ids, expected_status_counts):

    config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

    # use a test folder to store the database so production database is not affected
    if os.getcwd().endswith("unit_tests"):
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
        }
    else:
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
        }
    status_handler = SQLiteStatusHandler(config)

    # clean up before the test
    status_handler._delete_all_rows()

    status_rows = [
        StatusRow(
            target_id="dummy-id",
            target_url=f"dummy-url_{i}",
            status=statuses[i],
            source_batch_id=source_batch_ids[i],
            source_batch_name="dummy-source_batch_name",
            source_extra_info="something row-specific",
            proc_batch_id="dummy-proc_batch_id",
            proc_id=None,
            proc_status_msg=None,
            proc_error_code=None,
        )
        for i in range(len(statuses))
    ]

    status_handler.persist(status_rows)

    status_counts = status_handler.get_status_counts()

    for status in status_counts:
        assert ProcessingStatus(status) in expected_status_counts
        assert status_counts[status] == expected_status_counts[ProcessingStatus(status)]

    # clean up after the test
    status_handler._delete_all_rows()


@pytest.mark.parametrize(
    ("error_codes", "source_batch_ids", "expected_error_code_counts"),
    [
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 1, 1, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 4],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 3, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
        (
            [
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_ASSIGN_FAILED,
                ErrorCode.IMPOSSIBLE,
                ErrorCode.BATCH_REGISTER_FAILED,
            ],
            [1, 2, 1, 1],
            {
                ErrorCode.IMPOSSIBLE: 2,
                ErrorCode.BATCH_ASSIGN_FAILED: 1,
                ErrorCode.BATCH_REGISTER_FAILED: 1,
            },
        ),
    ],
)
def test_get_error_code_counts(
    config, error_codes, source_batch_ids, expected_error_code_counts
):

    config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

    # use a test folder to store the database so production database is not affected
    if os.getcwd().endswith("unit_tests"):
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
        }
    else:
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
        }

    status_handler = SQLiteStatusHandler(config)

    # clean up before the test
    status_handler._delete_all_rows()

    status_rows = [
        StatusRow(
            target_id="dummy-id",
            target_url=f"dummy-url_{i}",
            status=ProcessingStatus.NEW,
            source_batch_id=source_batch_ids[i],
            source_batch_name="dummy-source_batch_name",
            source_extra_info="something row-specific",
            proc_batch_id="dummy-proc_batch_id",
            proc_id=None,
            proc_status_msg=None,
            proc_error_code=error_codes[i],
        )
        for i in range(len(error_codes))
    ]

    status_handler.persist(status_rows)

    error_code_counts = status_handler.get_error_code_counts()

    for error_code in error_code_counts:
        assert ErrorCode(error_code) in expected_error_code_counts
        assert (
            error_code_counts[error_code]
            == expected_error_code_counts[ErrorCode(error_code)]
        )

    # clean up after the test
    status_handler._delete_all_rows()


@pytest.mark.parametrize(
    ("statuses", "extra_info_values", "expected_status_counts"),
    [
        (
            [ProcessingStatus.NEW, ProcessingStatus.NEW, ProcessingStatus.NEW],
            ["genre one", "genre one", "genre one"],
            {"genre one": {ProcessingStatus.NEW: 3}},
        ),
        (
            [ProcessingStatus.NEW, ProcessingStatus.NEW, ProcessingStatus.NEW],
            ["genre one", "genre two", "genre three"],
            {
                "genre one": {ProcessingStatus.NEW: 1},
                "genre two": {ProcessingStatus.NEW: 1},
                "genre three": {ProcessingStatus.NEW: 1},
            },
        ),
        (
            [ProcessingStatus.NEW, ProcessingStatus.ERROR, ProcessingStatus.FINISHED],
            ["genre one", "genre two", "genre three"],
            {
                "genre one": {ProcessingStatus.NEW: 1},
                "genre two": {ProcessingStatus.ERROR: 1},
                "genre three": {ProcessingStatus.FINISHED: 1},
            },
        ),
        (
            [
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.FINISHED,
                ProcessingStatus.NEW,
                ProcessingStatus.ERROR,
                ProcessingStatus.PROCESSING,
            ],
            [
                "genre one",
                "genre two",
                "genre three",
                "genre three",
                "genre two",
                "genre two",
            ],
            {
                "genre one": {ProcessingStatus.NEW: 1},
                "genre two": {
                    ProcessingStatus.ERROR: 2,
                    ProcessingStatus.PROCESSING: 1,
                },
                "genre three": {ProcessingStatus.NEW: 1, ProcessingStatus.FINISHED: 1},
            },
        ),
    ],
)
def test_get_status_counts_per_extra_info(
    config, statuses, extra_info_values, expected_status_counts
):

    config["STATUS_HANDLER"]["TYPE"] = "SQLiteStatusHandler"

    # use a test folder to store the database so production database is not affected
    if os.getcwd().endswith("unit_tests"):
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["..", "proc_stats", "all_stats.db"])
        }
    else:
        config["STATUS_HANDLER"]["CONFIG"] = {
            "DB_FILE": sep.join(["proc_stats", "all_stats.db"])
        }
    status_handler = SQLiteStatusHandler(config)

    # clean up before the test
    status_handler._delete_all_rows()

    status_rows = [
        StatusRow(
            target_id="dummy-id",
            target_url=f"dummy-url_{i}",
            status=statuses[i],
            source_batch_id="dummy-source-batch-id",
            source_batch_name="dummy-source_batch_name",
            source_extra_info=extra_info_values[i],
            proc_batch_id="dummy-proc_batch_id",
            proc_id=None,
            proc_status_msg=None,
            proc_error_code=None,
        )
        for i in range(len(statuses))
    ]

    status_handler.persist(status_rows)

    # get the status overviews
    status_counts_per_extra_info = (
        status_handler.get_status_counts_per_extra_info_value()
    )

    for extra_info_value in status_counts_per_extra_info:
        for status in status_counts_per_extra_info[extra_info_value]:
            assert ProcessingStatus(status) in expected_status_counts[extra_info_value]
            assert (
                status_counts_per_extra_info[extra_info_value][status]
                == expected_status_counts[extra_info_value][ProcessingStatus(status)]
            )

    # clean up after the test
    status_handler._delete_all_rows()
