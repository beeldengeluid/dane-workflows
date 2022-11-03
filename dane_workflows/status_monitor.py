from abc import ABC, abstractmethod
import json
import sys
import logging
from slack_sdk import WebClient

from dane_workflows.status import (
    StatusHandler,
    ExampleStatusHandler,
    ProcessingStatus,
    ErrorCode,
)
from dane_workflows.util.base_util import check_setting, load_config_or_die
from datetime import datetime

from dane_workflows.data_processing import (
    DataProcessingEnvironment,
    ExampleDataProcessingEnvironment,
)
from dane_workflows.exporter import Exporter, ExampleExporter


logger = logging.getLogger(__name__)


class StatusMonitor(ABC):
    def __init__(
        self,
        config: dict,
        status_handler: StatusHandler,
        data_processing_env: DataProcessingEnvironment,
        exporter: Exporter,
    ):
        self.status_handler = status_handler
        self.data_processing_env = data_processing_env
        self.exporter = exporter
        self.config = (
            config["STATUS_MONITOR"]["CONFIG"]
            if "CONFIG" in config["STATUS_MONITOR"]
            else {}
        )

        # enforce config validation
        if not self._validate_config():
            logger.critical("Malconfigured, quitting...")
            sys.exit()

    def _validate_config(self) -> bool:
        """Check that the config contains the necessary parameters"""
        logger.info(f"Validating {self.__class__.__name__} config")

        try:
            assert all(
                [x in self.config for x in ["INCLUDE_EXTRA_INFO"]]
            ), "StatusMonitor config misses required fields"

            assert check_setting(
                self.config["INCLUDE_EXTRA_INFO"], bool
            ), "StatusMonitor.INCLUDE_EXTRA_INFO not a bool"

        except AssertionError as e:
            logger.error(f"Configuration error: {str(e)}")
            return False

        return True

    def _check_status(self):
        """Collects status information about the tasks stored in the status_handler and returns it in a dict
        Returns: dict with status information

        "Last batch processed:" - processing batch ID of the last batch processed
        "Last batch processed :information_source: Status info:" - dict of statuses and their counts for the last batch processed
        "Last batch processed :warning: Error info:" - dict of error codes and their counts for the last batch processed

        "Last src batch retrieved:" - source batch ID of the last batch retrieved from the data provider
        "Last src batch retrieved :information_source: Status info:" - dict of error codes and their counts for the last batch
        retrieved from the data provider
        "Last src batch retrieved :warning: Error info:" - dict of statuses and their counts for the last batch
        retrieved from the data provider
        """

        last_proc_batch_id = self.status_handler.get_last_proc_batch_id()
        last_source_batch_id = self.status_handler.get_last_source_batch_id()

        logger.info(f"LAST PROC BATCH {last_proc_batch_id}")
        logger.info(f"LAST SOURCE BATCH {last_source_batch_id}")

        return {
            # get last batch retrieved
            "Last src batch retrieved": last_source_batch_id,
            # get last batch processed
            "Last batch processed": last_proc_batch_id,
            # get status and error code information for last batch retrieved
            "Last src batch retrieved :information_source: Status info": {
                f"{ProcessingStatus(status).name}": count
                for status, count in self.status_handler.get_status_counts_for_source_batch_id(
                    last_source_batch_id
                ).items()
            },
            # get status and error code information for last batch processed
            "Last batch processed :information_source: Status info": {
                f"{ProcessingStatus(status).name}": count
                for status, count in self.status_handler.get_status_counts_for_proc_batch_id(
                    last_proc_batch_id
                ).items()
            },
            "Last src batch retrieved :warning: Error info": {
                (f"{ErrorCode(error_code).name}" if error_code else "N/A"): count
                for error_code, count in self.status_handler.get_error_code_counts_for_source_batch_id(
                    last_source_batch_id
                ).items()
                if error_code
            },
            "Last batch processed :warning: Error info": (
                {
                    f"{ErrorCode(error_code).name}" if error_code else "N/A": count
                    for error_code, count in self.status_handler.get_error_code_counts_for_proc_batch_id(
                        last_proc_batch_id
                    ).items()
                    if error_code
                }
            ),
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

        status_report = {
            "Completed semantic source batch IDs": completed_batch_ids,
            "Uncompleted semantic source batch IDs": uncompleted_batch_ids,
            "Current semantic source batch ID": self.status_handler.get_name_of_source_batch_id(
                self.status_handler.get_cur_source_batch_id()
            ),
            "Status overview": self.status_handler.get_status_counts(),
            "Error overview": self.status_handler.get_error_code_counts(),
        }

        if include_extra_info:
            status_report[
                "Status overview per extra info"
            ] = self.status_handler.get_status_counts_per_extra_info_value()

        return status_report

    def _monitor_status(self):
        """Retrieves the status and error information and communicates this via the
        chosen method (implemented in _send_status())
        """
        status_info = self._check_status()
        satus_report = self._get_detailed_status_report(
            include_extra_info=self.config["INCLUDE_EXTRA_INFO"]
        )
        formatted_status_info = self._format_status_info(status_info)
        formatted_status_report = self._format_status_report(satus_report)
        self._send_status(formatted_status_info, formatted_status_report)

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
    def _format_status_report(self, status_report: dict):
        """Format the detailed status info
        Args:
        - status_report - detailed status information
        Returns:
        - formatted strings for the detailed error report
        """
        raise NotImplementedError("All StatusMonitors should implement this")

    @abstractmethod
    def _send_status(self, formatted_status: str, formatted_status_report: str = None):
        """Send status
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_status_report - Optional: a string containing the formatted error report
        Returns:
        """
        raise NotImplementedError("All StatusMonitors should implement this")


class ExampleStatusMonitor(StatusMonitor):
    def __init__(
        self,
        config: dict,
        status_handler: StatusHandler,
        data_processing_env: DataProcessingEnvironment,
        exporter: Exporter,
    ):
        super(ExampleStatusMonitor, self).__init__(
            config, status_handler, data_processing_env, exporter
        )

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

    def _format_status_report(self, status_report: dict):
        """Format the detailed status info as json
        Args:
        - stauts_report - detailed status information
        Returns:
        - formatted strings for the detailed stauts report
        """
        # basic superclass implementation is a json dump
        formatted_status_report = json.dumps(status_report)

        return formatted_status_report

    def _send_status(self, formatted_status: str, formatted_status_report: str = None):
        """Send status to terminal
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_status_report - Optional: a string containing the formatted error report
        Returns:
        """
        logger.info("STATUS INFO:")
        logger.info(formatted_status)
        logger.info("DETAILED STATUS REPORT:")
        logger.info(formatted_status_report)


class SlackStatusMonitor(StatusMonitor):
    def __init__(
        self,
        config: dict,
        status_handler: StatusHandler,
        data_processing_env: DataProcessingEnvironment,
        exporter: Exporter,
    ):
        super(SlackStatusMonitor, self).__init__(
            config, status_handler, data_processing_env, exporter
        )

    def _validate_config(self):
        """Check that the config contains the necessary parameters for Slack"""
        logger.info(f"Validating {self.__class__.__name__} config")

        if not StatusMonitor._validate_config(
            self
        ):  # if superclass validate fails, all fails
            logger.error("StatusMonitor default config section not valid")
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
                logger.error(f"Configuration error: {str(e)}")
                return False

        return True

    @staticmethod
    def _create_divider():
        """ " Create a divider block
        Returns:
        - returns a divider block as expected by the Slack API
        """
        return {"type": "divider"}

    @staticmethod
    def _create_basic_text_block(text):
        """Add a block containing text
        Args:
        - text:
            the text to put in the text block
        Returns:
        - returns the text block as expected by the Slack API
        """
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    @staticmethod
    def _create_markdown_fields_section_block(status_info: dict):
        """Add a block of type section containing a list of mrkdwn fields
        Args:
        - status_info_items:
            a list of items from status_info
        Returns:
        - returns the section block as expected by the Slack API
        """
        fields = []
        for key, value in status_info.items():
            match value:
                case str() as value:
                    text = f"*{key}:*\n{value}"
                case int() as value:
                    text = f"*{key}:*\n{value}"
                case {} as value:
                    if "error" in key.lower():
                        text = f"*{key}:*\nN/A :large_green_circle:"
                    else:
                        text = f"*{key}:*\nN/A"
                case dict() as value:
                    if "error" in key.lower():
                        text = f"*{key}:*\n:red_circle:\n"
                    else:
                        text = f"*{key}:*\n"
                    for status_or_error, count in value.items():
                        text += f"{status_or_error}: {count}\n"
                case _:
                    raise TypeError(
                        f"{type(value)} is of the wrong type or this type is not implemented"
                    )
            fields.append({"type": "mrkdwn", "text": text})
        return {"type": "section", "fields": fields}

    def _create_context_block(self):
        """Add a block of type context containing a mrkdwn field listing relevant values from config
        Args:
        - contextText: a string containing markdown formatted text
        Returns:
        - returns the context block as expected by the Slack API
        """
        statusDefinitionsURL = (
            "https://beng.slack.com/files/T03P55HJ9/F042WDNGD5W?origin_team=T03P55HJ9"
        )
        statusItems = {}
        # add config vars from data processing
        statusItems.update(self.data_processing_env.get_pretty_processing_conf_vars())
        # add config vars from exporter
        statusItems.update(self.exporter.get_pretty_export_conf_vars())
        # add status definitions URL
        statusItems["Definitions"]: statusDefinitionsURL

        contextTexts = [
            "*{}*: {}".format(key, value) for (key, value) in statusItems.items()
        ]
        contextText = "\n".join(contextTexts)
        return {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": contextText}],
        }

    def _format_status_info(self, status_info: dict):
        """Format the basis status information for slack
        Args:
        - status_info - the basic status information
        Returns:
        - formatted string for the basic status information
        """
        slack_status_info_list = []
        slack_status_info_list.append(self._create_divider())
        slack_status_info_list.append(
            self._create_basic_text_block(
                f'*Status report for workflow*:\n{self.config["WORKFLOW_NAME"]}'
            )
        )
        slack_status_info_list.append(
            self._create_markdown_fields_section_block(status_info)
        )
        slack_status_info_list.append(self._create_context_block())

        return slack_status_info_list

    def _format_status_report(self, status_report: dict):
        """Format the detailed status info for slack
        Args:
        - status_report - detailed status information
        Returns:
        - formatted strings for the detailed error report
        """
        return json.dumps(status_report)

    def _send_status(self, formatted_status, formatted_status_report: str = None):
        """Send status to slack
        Args:
        - formatted_status - a string containing the formatted status information
        - formatted_status_report - Optional: a string containing the formatted error report
        Returns:
        """
        slack_client = WebClient(self.config["TOKEN"])

        slack_client.chat_postMessage(
            channel=self.config["CHANNEL"],
            blocks=formatted_status,
        )

        if formatted_status_report:  # only upload error file if has content
            datetime_now = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            filename_status_report = "{}-{}.json".format(
                self.config["WORKFLOW_NAME"], datetime_now
            )
            slack_client.files_upload(
                content=formatted_status_report,
                filename=filename_status_report,
                channels=[self.config["CHANNEL"]],
                initial_comment="*Satus file* (based on current status database)",
            )


if __name__ == "__main__":
    """Call this to test your chosen StatusMonitor independently.
    It will then run on the status handler specified in the config"""

    config = load_config_or_die(
        "../config-example.yml"
    )  # TODO: how do we get this to work from within a workflow with the correct config?
    status_handler = ExampleStatusHandler(config)
    data_processing_env = ExampleDataProcessingEnvironment(config, status_handler)
    exporter = ExampleExporter(config, status_handler)
    status_monitor = SlackStatusMonitor(
        config, status_handler, data_processing_env, exporter
    )
    status_monitor._monitor_status()
