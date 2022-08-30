from abc import ABC, abstractmethod
import json
import sys

from slack_sdk import WebClient

from dane_workflows.status import (
    StatusHandler,
    ExampleStatusHandler,
    ProcessingStatus,
    ErrorCode,
)
from dane_workflows.util.base_util import get_logger, check_setting, load_config


class StatusMonitor(ABC):
    def __init__(self, config: dict, status_handler: StatusHandler):
        self.status_handler = status_handler

        # check if the configured TYPE is the same as the StatusMonitor being instantiated
        if self.__class__.__name__ != config["STATUS_MONITOR"]["TYPE"]:
            print("Malconfigured class instance")
            sys.exit()

        self.logger = get_logger(config)
        self.config = (
            config["STATUS_MONITOR"]["CONFIG"]
            if "CONFIG" in config["STATUS_MONITOR"]
            else {}
        )

        # enforce config validation
        if not self._validate_config():
            self.logger.error("Malconfigured, quitting...")
            sys.exit()

    def _validate_config(self) -> bool:
        """Check that the config contains the necessary parameters"""
        self.logger.debug(f"Validating {self.__class__.__name__} config")

        try:
            assert all(
                [x in self.config for x in ["INCLUDE_EXTRA_INFO"]]
            ), "STATUS_MONITOR.keys"

            assert check_setting(
                self.config["INCLUDE_EXTRA_INFO"], bool
            ), "StatusMonitor.INCLUDE_EXTRA_INFO"

        except AssertionError as e:
            self.logger.error(f"Configuration error: {str(e)}")
            return False

        return True

    def _check_status(self):
        """Collects status information about the tasks stored in the status_handler and returns it in a dict
        Returns: dict with status information
        "Last batch processed" - processing batch ID of the last batch processed
        "Last source batch retrieved" - source batch ID of the last batch retrieved from the data provider
        "Status information for last batch processed" - dict of statuses and their counts for the last batch processed
        "Error information for last batch processed"- dict of error codes and their counts for the last batch processed
        "Status information for last source batch retrieved" - dict of statuses and their counts for the last batch
        retrieved from the data provider
        "Error information for last source batch retrieved"- dict of error codes and their counts for the last batch
        retrieved from the data provider
        """

        last_proc_batch_id = self.status_handler.get_last_proc_batch_id()
        last_source_batch_id = self.status_handler.get_last_source_batch_id()

        print(f"LAST PROC BATCH {last_proc_batch_id}")
        print(f"LAST SOURCE BATCH {last_source_batch_id}")

        return {
            # get last batch processed
            "Last batch processed": last_proc_batch_id,
            # get last batch retrieved
            "Last source batch retrieved": last_source_batch_id,
            # get status and error code information for last batch processed
            "Status information for last batch processed": [
                f"{ProcessingStatus(status)}: {count}"
                for status, count in self.status_handler.get_status_counts_for_proc_batch_id(
                    last_proc_batch_id
                ).items()
            ],
            "Error information for last batch processed": [
                f"{ErrorCode(error_code)}: {count}" if error_code else "N/A"
                for error_code, count in self.status_handler.get_error_code_counts_for_proc_batch_id(
                    last_proc_batch_id
                ).items()
            ],
            # get status and error code information for last batch retrieved
            "Status information for last source batch retrieved": [
                f"{ProcessingStatus(status)}: {count}"
                for status, count in self.status_handler.get_status_counts_for_source_batch_id(
                    last_source_batch_id
                ).items()
            ],
            "Error information for last source batch retrieved": [
                f"{ErrorCode(error_code)}: {count}" if error_code else "N/A"
                for error_code, count in self.status_handler.get_error_code_counts_for_source_batch_id(
                    last_source_batch_id
                ).items()
            ],
        }

    def _get_detailed_status_report(self, include_extra_info):
        """Gets a detailed status report on all batches whose status is stored in the status_handler
        Args:
            - include_extra_info - if this is true, then an overview of statuses per value of the extra_info
            field in the StatusRow is returned
        Returns a dict of information:
        - "Completed semantic source batch IDs" - a list of all completed semantic source batch IDs
        - "Uncompleted semantic source batch IDs" - a list of all uncompleted semantic source batch IDs
        - "Current semantic source batch ID" - the semantic source batch currently being processed
        - "Status overview" - a dict with the statuses and their counts over all batches
        - "Error overview" - a dict with the error codes and their counts over all batches
        - "Status overview per extra info" - optional, if include_extra_info is true. A dict with status overview
        per value of the extra info field"""
        (
            completed_batch_ids,
            uncompleted_batch_ids,
        ) = self.status_handler.get_completed_semantic_source_batch_ids()

        error_report = {
            "Completed semantic source batch IDs": completed_batch_ids,
            "Uncompleted semantic source batch IDs": uncompleted_batch_ids,
            "Current semantic source batch ID": self.status_handler.get_name_of_source_batch_id(
                self.status_handler.get_cur_source_batch_id()
            ),
            "Status overview": self.status_handler.get_status_counts(),
            "Error overview": self.status_handler.get_error_code_counts(),
        }

        if include_extra_info:
            error_report[
                "Status overview per extra info"
            ] = self.status_handler.get_status_counts_per_extra_info_value()

        return error_report

    @abstractmethod
    def _format_status_info(self, status_info: dict):
        """Format the basis status information as json
        Args:
        - status_info - the basic status information
        Returns:
        - formatted string for the basic status information
        """
        # basic superclass implementation is a json dump
        formatted_status_info = json.dumps(status_info)

        return formatted_status_info

    @abstractmethod
    def _format_error_report(self, error_report: dict):
        """Format the detailed status info
        Args:
        - error_report - detailed status information
        Returns:
        - formatted strings for the detailed error report
        """
        raise NotImplementedError("All StatusMonitors should implement this")

    @abstractmethod
    def _send_status(self, formatted_status: str, formatted_error_report: str = None):
        """Send status
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_error_report - Optional: a string containing the formatted error report
        Returns:
        """
        raise NotImplementedError("All StatusMonitors should implement this")

    @abstractmethod
    def monitor_status(self):
        """Retrieves the status and error information and communicates this via the
        chosen method (implemented in _send_status())
        """
        raise NotImplementedError("All StatusMonitors should implement this")


class ExampleStatusMonitor(StatusMonitor):
    def __init__(self, config: dict, status_handler: StatusHandler):
        super(ExampleStatusMonitor, self).__init__(config, status_handler)

    def _validate_config(self):
        return StatusMonitor._validate_config(self)  # no additional config needed

    def _format_status_info(self, status_info: dict) -> str:
        """Format the basis status information as json
        Args:
        - status_info - the basic status information
        with
        Returns:
        - formatted string for the basic status information
        """
        # basic superclass implementation is a json dump
        formatted_status_info = json.dumps(status_info)
        return formatted_status_info

    def _format_error_report(self, error_report: dict):
        """Format the detailed status info as json
        Args:
        - error_report - detailed status information
        Returns:
        - formatted strings for the detailed error report
        """
        # basic superclass implementation is a json dump
        formatted_error_report = json.dumps(error_report)

        return formatted_error_report

    def _send_status(self, formatted_status: str, formatted_error_report: str = None):
        """Send status to terminal
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_error_report - Optional: a string containing the formatted error report
        Returns:
        """
        print("STATUS INFO:")
        print(formatted_status)
        print("DETAILED ERROR REPORT:")
        print(formatted_error_report)

    def monitor_status(self):
        """Retrieves the status and error information and communicates this via the terminal"""
        status_info = self._check_status()
        error_report = self._get_detailed_status_report(status_info)
        formatted_status_info = self._format_status_info(status_info)
        formatted_error_report = self._format_error_report(error_report)
        self._send_status(formatted_status_info, formatted_error_report)


class SlackStatusMonitor(StatusMonitor):
    def __init__(self, config: dict, status_handler: StatusHandler):
        super(SlackStatusMonitor, self).__init__(config, status_handler)

    def _validate_config(self):
        """Check that the config contains the necessary parameters for Slack"""
        self.logger.debug(f"Validating {self.__class__.__name__} config")

        if not StatusMonitor._validate_config(
            self
        ):  # if superclass validate fails, all fails
            return False
        else:
            try:
                assert all(
                    [x in self.config for x in ["TOKEN", "CHANNEL", "WORKFLOW_NAME"]]
                ), "STATUS_MONITOR.keys"

                assert check_setting(
                    self.config["TOKEN"], str
                ), "SlackStatusMonitor.TOKEN"

                assert check_setting(
                    self.config["CHANNEL"], str
                ), "SlackStatusMonitor.CHANNEL"

                assert check_setting(
                    self.config["WORKFLOW_NAME"], str
                ), "SlackStatusMonitor.WORKFLOW_NAME"

            except AssertionError as e:
                self.logger.error(f"Configuration error: {str(e)}")
                return False

        return True

    @staticmethod
    def _create_divider():
        """ " Create a divider block
        Returns:
        - returns a divider block
        """
        return {"type": "divider"}

    @staticmethod
    def _create_basic_text_block(text):
        """Add a block containing text
        Args:
        - text:
            the text to put in the text block
        Returns:
        - returns the text block
        """
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    def _format_status_info(self, status_info: dict):
        """Format the basis status information for slack
        Args:
        - status_info - the basic status information
        Returns:
        - formatted string for the basic status information
        """
        slack_status_info_list = []
        slack_status_info_list.append(
            self._create_basic_text_block(
                f'*{self.config["WORKFLOW_NAME"]} STATUS REPORT*'
            )
        )
        for key, value in status_info.items():
            match value:
                case str() as value:
                    text = f"*{key}*: {value}"
                case int() as value:
                    text = f"*{key}*: {value}"
                case dict() as value:
                    text = f"*{key}*\n"
                    for status_or_error, count in value.items():
                        text += f"{status_or_error}: {count}\n"
                case _:
                    raise TypeError(
                        f"{type(value)} is of the wrong type or this type is not implemented"
                    )
            slack_status_info_list.append(self._create_divider())
            slack_status_info_list.append(self._create_basic_text_block(text))

        return slack_status_info_list

    def _format_error_report(self, error_report: dict):
        """Format the detailed status info for slack
        Args:
        - error_report - detailed status information
        Returns:
        - formatted strings for the detailed error report
        """
        return json.dumps(error_report)

    def _send_status(self, formatted_status, formatted_error_report: str = None):
        """Send status to slack
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_error_report - Optional: a string containing the formatted error report
        Returns:
        """
        slack_client = WebClient(self.config["TOKEN"])

        slack_client.chat_postMessage(
            channel=self.config["CHANNEL"],
            blocks=formatted_status,
            icon_emoji=":ghost:",
        )

        if formatted_error_report:  # only upload error file if has content
            slack_client.files_upload(
                content=formatted_error_report,
                channels=[self.config["CHANNEL"]],
                initial_comment="For more details, review this error file",
            )

    def monitor_status(self):
        """Retrieves the status and error information and communicates this via the terminal"""
        status_info = self._check_status()
        error_report = self._get_detailed_status_report(
            include_extra_info=self.config["INCLUDE_EXTRA_INFO"]
        )
        formatted_status_info = self._format_status_info(status_info)
        formatted_error_report = self._format_error_report(error_report)
        self._send_status(formatted_status_info, formatted_error_report)


if __name__ == "__main__":

    """Call this to test your chosen StatusMonitor independently.
    It will then run on the status handler specified in the config"""

    config = load_config(
        "../config-example.yml"
    )  # TODO: how do we get this to work from within a workflow with the correct config?
    status_handler = ExampleStatusHandler(config)
    status_monitor = SlackStatusMonitor(config, status_handler)
