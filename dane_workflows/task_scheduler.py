# from abc import ABC, abstractmethod
from typing import List, Type, Tuple
import dane_workflows.util.base_util as base_util
from dane_workflows.data_provider import DataProvider, ProcessingStatus
from dane_workflows.data_processing import DataProcessingEnvironment
from dane_workflows.exporter import Exporter
from dane_workflows.status import StatusHandler, StatusRow, ErrorCode


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

        # first initialize the status handler and pass it to the data provider and processing env
        self.status_handler: StatusHandler = status_handler(config)
        self.data_provider = data_provider(
            config, self.status_handler, unit_test
        )  # instantiate the DataProvider
        self.data_processing_env = data_processing_env(
            config, self.status_handler, unit_test
        )  # instantiate the DataProcessingEnvironment
        self.exporter = exporter(config, self.status_handler, unit_test)

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

    def _recover(self) -> Tuple[List[StatusRow], int, int]:
        source_batch_recovered, last_proc_batch = self.status_handler.recover(
            self.data_provider
        )
        if source_batch_recovered is False:
            self.logger.info("Could not recover any source batch, quitting")
            quit()

        last_proc_batch_id = 0
        skip_steps = 0

        if last_proc_batch:
            last_proc_batch_id = self.status_handler.get_last_proc_batch_id()
            self.logger.info("Synchronizing last proc_batch with ProcessingEnvironment")

            # determine where to resume processing by looking at the highest step in the chain
            highest_proc_stat = 0
            for row in last_proc_batch:
                if row.status == ProcessingStatus.ERROR:  # skip errors
                    continue
                if row.status.value > highest_proc_stat:
                    highest_proc_stat = row.status.value
            skip_steps = highest_proc_stat - 2

        return last_proc_batch, last_proc_batch_id, skip_steps

    # TODO implement proper recovery
    def run(self):

        # always try to recover (without status files, the first source batch will be created)
        last_proc_batch, last_proc_batch_id, skip_steps = self._recover()

        # if a proc_batch was recovered, make sure to finish it from the last ProcessingStatus
        if last_proc_batch:
            if self.process_proc_batch(last_proc_batch, skip_steps) is True:
                last_proc_batch_id += 1  # continue on
            else:
                self.logger.critical("Critical error whilst processing, quitting")

        proc_batch_id = last_proc_batch_id

        # continue until all is finished or something breaks
        while True:
            # first get the batch from the data provider
            self.logger.debug(
                f"asking DP for next batch: {proc_batch_id} ({self.BATCH_SIZE})"
            )
            status_rows_dp = self.data_provider.get_next_batch(
                proc_batch_id, self.BATCH_SIZE
            )
            if status_rows_dp is None:
                self.logger.debug("No source data remaining, all done, quitting...")
                break

            if self.process_proc_batch(status_rows_dp, proc_batch_id) is False:
                self.logger.critical("Critical error whilst processing, quitting")
                break

            # update the proc_batch_id and continue on
            proc_batch_id += 1

    def process_proc_batch(
        self, status_rows: List[StatusRow], proc_batch_id: int, skip_steps: int = 0
    ) -> bool:
        # then register the batch in the data processing environment
        self.logger.debug(f"Registering batch: {proc_batch_id} ({self.BATCH_SIZE})")

        if skip_steps == 0:  # first register the batch in the proc env
            status_rows_rb = self.data_processing_env.register_batch(
                proc_batch_id, status_rows
            )
            if status_rows_rb is None:
                self.logger.error(f"Could not register batch {proc_batch_id}, quitting")
                return False

        if skip_steps < 2:  # Alright let's ask the proc env to start processing
            self.logger.debug(
                f"Successfully registered batch: {proc_batch_id} ({self.BATCH_SIZE})"
            )
            status_rows_pb = self.data_processing_env.process_batch(proc_batch_id)

            if status_rows_pb is None:
                self.logger.error(f"Could not process batch {proc_batch_id}, quitting")
                return False
            else:
                self.logger.info(
                    f"Successfully triggered the process for: {proc_batch_id} ({self.BATCH_SIZE})"
                )

        if skip_steps < 3:  # monitor the processing, until it returns the results
            self.logger.info(
                f"Start monitoring proc batch until it finishes: {proc_batch_id} ({self.BATCH_SIZE})"
            )
            status_rows_m = self.data_processing_env.monitor_batch(proc_batch_id)
            if status_rows_m is None:
                self.logger.error(
                    f"Something went wrong whilst monitoring proc_batch: {proc_batch_id}, quitting"
                )
                return False
            else:
                self.logger.debug(
                    f"Received monitoring status results for proc_batch: {proc_batch_id}"
                )

        if skip_steps < 4:  # now fetch the results from the ProcessingEnvironment
            processing_results = self.data_processing_env.fetch_results_of_batch(
                proc_batch_id
            )
            if processing_results is None:
                self.logger.error(
                    f"Did not receive any processing results for {proc_batch_id}, quitting"
                )
                return False

        if skip_steps < 5:  # finally have the exporter export the results
            if self.exporter.export_results(processing_results):
                self.logger.info("Successfully exported results back to source")
            else:
                self.logger.warning("Could not export results back to source")

        return True


# test a full workflow
if __name__ == "__main__":
    from dane_workflows.util.base_util import load_config
    from dane_workflows.status import SQLiteStatusHandler
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
