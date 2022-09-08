import pytest
from mockito import when, verify, ANY
import slack_sdk

from dane_workflows.status_monitor import ExampleStatusMonitor, SlackStatusMonitor

from dane_workflows.status import (
    ExampleStatusHandler,
    ProcessingStatus,
    ErrorCode,
)


""" --------------------- Example Status Monitor Tests ------------------ """


def test__check_status(config):
    status_handler = ExampleStatusHandler(config)
    status_monitor = ExampleStatusMonitor(config, status_handler)
    dummy_last_proc_batch_id = 1
    dummy_last_source_batch_id = 2

    dummy_error_code_counts_for_proc_batch = {
        ErrorCode.IMPOSSIBLE.value: 2,
        ErrorCode.BATCH_REGISTER_FAILED.value: 1,
    }

    dummy_status_counts_for_proc_batch = {
        ProcessingStatus.ERROR.value: 3,
        ProcessingStatus.NEW.value: 1,
        ProcessingStatus.FINISHED.value: 2,
    }
    dummy_error_code_counts_for_source_batch = {
        ErrorCode.IMPOSSIBLE.value: 1,
        ErrorCode.EXPORT_FAILED_SOURCE_DB_CONNECTION_FAILURE.value: 2,
        ErrorCode.EXPORT_FAILED_SOURCE_DOC_NOT_FOUND.value: 1,
    }

    dummy_status_counts_for_source_batch = {
        ProcessingStatus.ERROR.value: 4,
        ProcessingStatus.NEW.value: 5,
        ProcessingStatus.BATCH_ASSIGNED.value: 2,
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

        status_info = status_monitor._check_status()

        assert status_info["Last batch processed"] == dummy_last_proc_batch_id
        assert status_info["Last source batch retrieved"] == dummy_last_source_batch_id

        for key in dummy_status_counts_for_proc_batch:
            assert f"\'{ProcessingStatus(key).name}\': {dummy_status_counts_for_proc_batch[key]}" in str(
                status_info
            )
        for key in dummy_error_code_counts_for_proc_batch:
            assert f"\'{ErrorCode(key).name}\': {dummy_error_code_counts_for_proc_batch[key]}" in str(
                status_info
            )
        for key in dummy_status_counts_for_source_batch:
            assert f"\'{ProcessingStatus(key).name}\': {dummy_status_counts_for_source_batch[key]}" in str(
                status_info
            )
        for key in dummy_error_code_counts_for_source_batch:
            assert f"\'{ErrorCode(key).name}\': {dummy_error_code_counts_for_source_batch[key]}" in str(
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
def test__get_detailed_status_report(config, include_extra_info):
    status_handler = ExampleStatusHandler(config)
    status_monitor = ExampleStatusMonitor(config, status_handler)

    dummy_incomplete = ["dummy-incomplete-1", "dummy-incomplete-2"]
    dummy_complete = [
        "dummy-complete-1",
        "dummy-complete-2",
        "dummy-complete-3",
        "dummy-complete-4",
    ]
    dummy_source_batch_id = "dummy-source-batch-id"
    dummy_semantic_source_batch_name = "dummy-semantic-source-batch-name"
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
        ExampleStatusHandler
    ).get_cur_source_batch_id().thenReturn(
        dummy_source_batch_id
    ), when(
        ExampleStatusHandler
    ).get_name_of_source_batch_id(
        dummy_source_batch_id
    ).thenReturn(
        dummy_semantic_source_batch_name
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

        status_report = status_monitor._get_detailed_status_report(include_extra_info)

        assert "Completed semantic source batch IDs" in status_report
        assert status_report["Completed semantic source batch IDs"] == dummy_complete
        assert "Uncompleted semantic source batch IDs" in status_report
        assert (
            status_report["Uncompleted semantic source batch IDs"] == dummy_incomplete
        )
        assert "Current semantic source batch ID" in status_report
        assert (
            status_report["Current semantic source batch ID"]
            == dummy_semantic_source_batch_name
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
        verify(ExampleStatusHandler, times=1).get_cur_source_batch_id()
        verify(ExampleStatusHandler, times=1).get_name_of_source_batch_id(
            dummy_source_batch_id
        )
        verify(ExampleStatusHandler, times=1).get_status_counts()
        verify(ExampleStatusHandler, times=1).get_error_code_counts()
        verify(
            ExampleStatusHandler, times=1 if include_extra_info else 0
        ).get_status_counts_per_extra_info_value()


""" --------------------- Slack Status Monitor Tests ------------------ """


@pytest.mark.parametrize(
    ("token", "channel", "workflow_name", "include_extra_info", "expect_error"),
    [
        (None, None, None, None, True),
        ("dummy-token", None, None, None, True),
        (None, "dummy-channel", None, None, True),
        (None, None, "dummy-name", None, True),
        (None, None, None, False, True),
        ({}, "dummy-channel", "dummy-name", False, True),
        ("dummy-token", 123, "dummy-name", False, True),
        ("dummy-token", "dummy-channel", 5.6, False, True),
        ("dummy-token", "dummy-channel", 5.6, "False", True),
        ("dummy-token", "dummy-channel", "dummy-name", False, False),
    ],
)
def test_validate_config(
    config, token, channel, workflow_name, include_extra_info, expect_error
):
    config_to_validate = config
    config_to_validate["STATUS_MONITOR"]["TYPE"] = "SlackStatusMonitor"
    config_to_validate["STATUS_MONITOR"]["CONFIG"] = {}
    if token:
        config_to_validate["STATUS_MONITOR"]["CONFIG"]["TOKEN"] = token
    if channel:
        config_to_validate["STATUS_MONITOR"]["CONFIG"]["CHANNEL"] = channel
    if workflow_name:
        config_to_validate["STATUS_MONITOR"]["CONFIG"]["WORKFLOW_NAME"] = workflow_name
    if include_extra_info is not None:
        config_to_validate["STATUS_MONITOR"]["CONFIG"][
            "INCLUDE_EXTRA_INFO"
        ] = include_extra_info

    status_handler = ExampleStatusHandler(config_to_validate)

    if expect_error:
        with pytest.raises(SystemExit):
            SlackStatusMonitor(config_to_validate, status_handler)
    else:
        assert SlackStatusMonitor(config_to_validate, status_handler)


@pytest.mark.parametrize(
    ("status_info", "expected_output"),
    [
        (
            {
                "Last batch processed": 12345,
                "Status information for last batch processed": {
                    "STATUS INFO 1": 12,
                    "STATUS INFO 2": 4,
                },
            },
            [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*TESTING STATUS REPORT*"},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Last batch processed*: 12345"},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Status information for last batch processed*\nSTATUS INFO 1: 12\nSTATUS INFO 2: 4\n",
                    },
                },
            ],
        )
    ],
)
def test_format_status_info(slack_monitor_config, status_info: dict, expected_output):
    status_handler = ExampleStatusHandler(slack_monitor_config)
    slack_status_monitor = SlackStatusMonitor(slack_monitor_config, status_handler)
    status_info_list = slack_status_monitor._format_status_info(status_info)
    assert type(status_info_list) == list
    assert status_info_list == expected_output


@pytest.mark.parametrize(
    "formatted_error_report", [None, "a formatted error report string"]
)
def test__send_status(config, formatted_error_report):
    status_handler = ExampleStatusHandler(config)
    config["STATUS_MONITOR"]["TYPE"] = "SlackStatusMonitor"
    config["STATUS_MONITOR"]["CONFIG"] = {
        "TOKEN": "a token",
        "CHANNEL": "a channel",
        "WORKFLOW_NAME": "a workflow name",
        "INCLUDE_EXTRA_INFO": False,
    }
    status_monitor = SlackStatusMonitor(config, status_handler)
    dummy_formatted_status = "dummy formatted status"

    with when(slack_sdk.WebClient).chat_postMessage(
        channel=ANY, blocks=dummy_formatted_status, icon_emoji=ANY
    ), when(slack_sdk.WebClient).files_upload(
        content=formatted_error_report, channels=ANY, initial_comment=ANY
    ):

        status_monitor._send_status(dummy_formatted_status, formatted_error_report)

        verify(slack_sdk.WebClient, times=1).chat_postMessage(
            channel=ANY, blocks=dummy_formatted_status, icon_emoji=ANY
        )
        verify(
            slack_sdk.WebClient, times=1 if formatted_error_report else 0
        ).files_upload(
            content=formatted_error_report, channels=ANY, initial_comment=ANY
        )


@pytest.mark.parametrize("include_extra_info", [False, True])
def test_monitor_status(config, include_extra_info):
    status_handler = ExampleStatusHandler(config)
    config["STATUS_MONITOR"]["TYPE"] = "SlackStatusMonitor"
    config["STATUS_MONITOR"]["CONFIG"] = {
        "TOKEN": "a token",
        "CHANNEL": "a channel",
        "WORKFLOW_NAME": "a workflow name",
        "INCLUDE_EXTRA_INFO": include_extra_info,
    }
    status_monitor = SlackStatusMonitor(config, status_handler)

    dummy_status = {"dummy-key": "dummy-value"}
    dummy_error_report = {"dummy-error-key": "dummy-error-value"}
    dummy_formatted_status_info = "dummy formatted info"
    dummy_formatted_error_report = "dummy formatted error report"

    with when(status_monitor)._check_status().thenReturn(dummy_status), when(
        status_monitor
    )._get_detailed_status_report(include_extra_info=include_extra_info).thenReturn(
        dummy_error_report
    ), when(
        status_monitor
    )._format_status_info(
        dummy_status
    ).thenReturn(
        dummy_formatted_status_info
    ), when(
        status_monitor
    )._format_error_report(
        dummy_error_report
    ).thenReturn(
        dummy_formatted_error_report
    ), when(
        status_monitor
    )._send_status(
        dummy_formatted_status_info, dummy_formatted_error_report
    ):

        status_monitor.monitor_status()

        verify(status_monitor, times=1)._check_status()
        verify(status_monitor, times=1)._get_detailed_status_report(
            include_extra_info=include_extra_info
        )
        verify(status_monitor, times=1)._format_status_info(dummy_status)
        verify(status_monitor, times=1)._format_error_report(dummy_error_report)
        verify(status_monitor, times=1)._send_status(
            dummy_formatted_status_info, dummy_formatted_error_report
        )
