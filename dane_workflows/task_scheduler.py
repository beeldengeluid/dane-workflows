# from abc import ABC, abstractmethod
import datetime
from typing import List, Type
import dane_workflows.util.base_util as base_util
from dane_workflows.data_provider import DataProvider, ProcessingStatus
from dane_workflows.data_processing import DataProcessingEnvironment
from dane_workflows.exporter import Exporter
from dane_workflows.util.status_util import StatusHandler, StatusRow, ErrorCode


"""
Cron jobs should call an instance of this class and call its run() method
"""


# TODO add exporter, so results can be written back to the source
class TaskScheduler(object):
    def __init__(
        self,
        config: dict,
        status_handler: Type[StatusHandler],
        data_provider: Type[DataProvider],
        data_processing_env: Type[DataProcessingEnvironment],
        exporter: Type[Exporter],
        unit_test: bool = False,
    ):
        self.config = config

        if not self._validate_config():
            print("Malconfigured, quitting...")
            quit()

        self.BATCH_SIZE = config["TASK_SCHEDULER"]["BATCH_SIZE"]
        self.BATCH_PREFIX = config["TASK_SCHEDULER"][
            "BATCH_PREFIX"
        ]  # to keep track of the batches

        self.logger = base_util.init_logger(config)  # first init the logger
        self.date_started = datetime.datetime.now()

        # first initialize the status handler and pass it to the data provider and processing env
        self.status_handler: StatusHandler = status_handler(config)
        self.data_provider = data_provider(
            config, self.status_handler, unit_test
        )  # instantiate the DataProvider
        self.data_processing_env = data_processing_env(
            config, self.status_handler, unit_test
        )  # instantiate the DataProcessingEnvironment
        self.exporter = exporter(config, self.status_handler, unit_test)
        # always try to recover (without status files, the first source batch will be created)
        if unit_test is False:
            # TODO this should fetch the last source_batch_id and the last proc_batch_id, then:
            # -

            source_batch_recovered, last_proc_batch = self.status_handler.recover()

            self.logger.info(f"Recovered last status: {source_batch_recovered}")
            if source_batch_recovered is False:
                self.logger.info("No status data could be recovered, starting afresh")
                status_rows = self.data_provider.fetch_source_batch_data(0)
                if status_rows is not None:
                    self.logger.info("Starting from the first source batch")
                    self.status_handler.set_current_source_batch(status_rows)
                else:
                    self.logger.error("DataProvider could not fetch any initial data")
                    quit()

    def _validate_config(self):
        parent_dirs_to_check = []
        try:
            # check logging
            assert "LOGGING" in self.config, "LOGGING"
            assert all(
                [x in self.config["LOGGING"] for x in ["NAME", "DIR", "LEVEL"]]
            ), "LOGGING.keys"
            assert base_util.check_setting(
                self.config["LOGGING"]["LEVEL"], str
            ), "LOGGING.LEVEL"
            assert base_util.check_log_level(
                self.config["LOGGING"]["LEVEL"]
            ), "Invalid LOGGING.LEVEL defined"
            assert base_util.check_setting(
                self.config["LOGGING"]["DIR"], str
            ), "LOGGING.DIR"
            parent_dirs_to_check.append(self.config["LOGGING"]["DIR"])

            # check settings for this class
            assert "TASK_SCHEDULER" in self.config, "TASK_SCHEDULER"
            assert all(
                [
                    x in self.config["TASK_SCHEDULER"]
                    for x in ["BATCH_SIZE", "BATCH_PREFIX"]
                ]
            ), "TASK_SCHEDULER.keys"
            assert base_util.check_setting(
                self.config["TASK_SCHEDULER"]["BATCH_SIZE"], int
            ), "TASK_SCHEDULER.BATCH_SIZE"
            assert base_util.check_setting(
                self.config["TASK_SCHEDULER"]["BATCH_PREFIX"], str
            ), "TASK_SCHEDULER.BATCH_PREFIX"

            base_util.validate_parent_dirs(parent_dirs_to_check)
        except AssertionError as e:
            print(f"Configuration error: {str(e)}")
            return False

        return True

    """ -------------------------- RUN() ------------------------------------------------------- """

    def _to_dane_batch_name(self, proc_batch_id: int) -> str:
        return f"{self.BATCH_PREFIX}__{proc_batch_id}"

    def _update_status(
        self,
        status_rows: List[StatusRow],
        status: ProcessingStatus,
        proc_batch_id: int = -1,
        proc_status_msg: str = None,
        proc_error_code: ErrorCode = None,
    ):
        self.status_handler.persist(
            self.status_handler.update_status_rows(
                status_rows,
                status=status,
                proc_batch_id=proc_batch_id,
                proc_status_msg=proc_status_msg,
                proc_error_code=proc_error_code,
            )
        )

    def check_status(self):
        """Collects status information about this TaskScheduler and returns it in a dict
        Returns: dict with status information
        "Date started"  - the date the TaskScheduler was initialised
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

        return {
            # get date started
            "Date started": self.date_started.strftime("%Y-%m-%d"),
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

    def get_detailed_status_report(self, include_extra_info):
        """Gets a detailed status report on all batches completed by this TaskScheduler
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
            "Current semantic source batch ID": self.data_provider._to_semantic_source_batch_id(
                self.status_handler.get_last_source_batch_id()
            ),
            "Status overview": self.status_handler.get_status_counts(),
            "Error overview": self.status_handler.get_error_code_counts(),
        }

        if include_extra_info:
            error_report[
                "Status overview per extra info"
            ] = self.status_handler.get_status_counts_per_extra_info_value()

        return error_report

    # TODO implement proper recovery
    def run(self):
        #  fetch the last proc_batch_id via the StatusHandler
        proc_batch_id = self.status_handler.get_last_proc_batch_id()
        if proc_batch_id == -1:
            self.logger.info("Could not find a proc batch id, starting anew")
            proc_batch_id = 0
        # batch_name = self._to_dane_batch_name(proc_batch_id)
        # TODO check if the latest batch was completed, otherwise rerun it, e.g.:
        # if self.data_processing_env.is_proc_batch_complete(proc_batch_id):
        #     proc_batch_id += 1
        # else:
        #     self.data_processing_env.reset_dane_batch(proc_batch_id)
        while True:
            # first get the batch from the data provider
            self.logger.debug(
                f"asking DP for next batch: {proc_batch_id} ({self.BATCH_SIZE})"
            )
            status_rows_dp = self.data_provider.get_next_batch(
                proc_batch_id, self.BATCH_SIZE
            )
            if status_rows_dp is None:
                self.logger.debug("No source batch remaining, quitting...")
                break

            # then register the batch in the data processing environment
            self.logger.debug(f"register_batch: {proc_batch_id} ({self.BATCH_SIZE})")
            status_rows_dpe = self.data_processing_env.register_batch(
                proc_batch_id, status_rows_dp
            )
            if status_rows_dpe is not None:
                # now contains the proc_id so now we have a reference ID in the ProcessingEnvironment
                self.status_handler.persist(status_rows_dpe)
                # self.logger.debug("BREAKING OFF THE LOOP TO TEST")
                # break

                proc_resp = self.data_processing_env.process_batch(proc_batch_id)
                # start the processing
                if proc_resp.success:
                    self._update_status(
                        status_rows_dpe,
                        status=ProcessingStatus.PROCESSING,
                        proc_batch_id=proc_batch_id,
                        proc_status_msg=proc_resp.status_text,
                    )
                    self.logger.debug(
                        f"process_batch: {proc_batch_id} ({self.BATCH_SIZE})"
                    )

                    # monitor the processing, until it returns the results
                    self.logger.debug(
                        f"data_processing_env: {proc_batch_id} ({self.BATCH_SIZE})"
                    )

                    # TODO the monitor function still needs to return a list of updated status_rows
                    status_rows_monitor = self.data_processing_env.monitor_batch(
                        proc_batch_id
                    )
                    if status_rows_monitor:
                        # update the data provider with the processing status for each document in the batch
                        self.logger.debug(f"Received results for batch {proc_batch_id}")
                        self.status_handler.persist(status_rows_monitor)

                        # now fetch the results from the ProcessingEnvironment
                        processing_results = (
                            self.data_processing_env.fetch_results_of_batch(
                                proc_batch_id
                            )
                        )

                        if processing_results:
                            # have the exporter export the results
                            self.exporter.export_results(processing_results)
                            # TODO function should return something the TH can react on
                        else:
                            self.logger.error(
                                f"Did not receive any processing results for {proc_batch_id}"
                            )
                            break
                    else:
                        self.logger.error(
                            f"Did not receive any results for batch {proc_batch_id}"
                        )
                        break
                else:
                    self._update_status(
                        status_rows_dpe,
                        status=ProcessingStatus.ERROR,
                        proc_batch_id=proc_batch_id,
                        proc_status_msg=proc_resp.status_text,
                        proc_error_code=ErrorCode.BATCH_PROCESSING_NOT_STARTED,
                    )
                    self.logger.error(f"Could not process batch {proc_batch_id}")
                    break  # finish the loop for now

                # update the proc_batch_id and continue on
                proc_batch_id += 1

            else:
                self.logger.error(f"Could not register batch {proc_batch_id}")
                self._update_status(
                    status_rows_dp,
                    status=ProcessingStatus.ERROR,
                    proc_batch_id=proc_batch_id,
                    proc_status_msg=f"Could not register batch {proc_batch_id}",
                    proc_error_code=ErrorCode.BATCH_REGISTER_FAILED,
                )
                break

            # break  # finish the loop after one iteration for now


# test a full workflow
if __name__ == "__main__":
    from dane_workflows.util.base_util import load_config
    from dane_workflows.util.status_util import SQLiteStatusHandler
    from dane_workflows.data_provider import ExampleDataProvider
    from dane_workflows.data_processing import (
        ExampleDataProcessingEnvironment,
        # DANEEnvironment,
    )
    from dane_workflows.exporter import ExampleExporter

    print("Starting task scheduler")
    config = load_config("../config.yml")

    # print(config)

    ts = TaskScheduler(
        config,
        SQLiteStatusHandler,
        ExampleDataProvider,
        ExampleDataProcessingEnvironment,
        ExampleExporter,
    )

    ts.run()
