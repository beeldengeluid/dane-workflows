from dane_workflows.status import StatusHandler, ProcessingStatus, ErrorCode
import datetime


class StatusMonitor:
    def __init__(
        self, config: dict, status_handler: StatusHandler
    ):
        self.status_handler = status_handler

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
                f"{ErrorCode(error_code)}: {count}"
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
                f"{ErrorCode(error_code)}: {count}"
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
