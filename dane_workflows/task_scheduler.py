from typing import List, Type, Tuple, Optional
import dane_workflows.util.base_util as base_util
from dane_workflows.data_provider import DataProvider, ProcessingStatus
from dane_workflows.data_processing import DataProcessingEnvironment, ProcessingResult
from dane_workflows.exporter import Exporter
from dane_workflows.status import StatusHandler, StatusRow


"""
The TaskScheduler is the main process that interprets & runs a workflow comprised of:
- implementation of a DataProvider
- implementation of a ProcessingEnvironment
- implementation of an Exporter
- implementation of a StatusHandler

The constructor takes a config and a class type for each of the aforementioned components
to be able to instantiate the correct implementions. The config should make sure to provide
these implementations with the specific parameters they require.
"""


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

    # Calls the StatusHandler to load the status_handler.cur_source_batch into memory.
    #
    # Subsequently the StatusHandler is called to recover the last proc_batch.
    #
    # Based on the last ProcessingStatus in this proc_batch the number of steps to skip
    # within _run_proc_batch() is determined to resume processing from
    def _recover(self) -> Tuple[Optional[List[StatusRow]], int, int]:
        source_batch_recovered, last_proc_batch = self.status_handler.recover(
            self.data_provider
        )
        if source_batch_recovered is False:
            self.logger.info(
                "Could not recover source_batch, so either the work was done or something is wrong with the DataProvider, quitting"
            )
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

    # Before starting the endless loop of processing everything the DataProvider has to offer,
    # _recover() is called to make sure:
    #
    # 1. The StatusHandler has loaded cur_source_batch in memory
    # 2. The last proc_batch is retrieved (representing the batch last fed to the ProcessingEnvironment)
    # 3. The last successful step within this batch is retrieved we know how many steps to
    #    skip within _run_proc_batch()
    def run(self):

        # always try to recover (without StatusHandler data, the first source_batch will be created)
        last_proc_batch, last_proc_batch_id, skip_steps = self._recover()

        # if a proc_batch was recovered, make sure to finish it from the last ProcessingStatus
        if last_proc_batch:
            if self._run_proc_batch(last_proc_batch, skip_steps) is True:
                last_proc_batch_id += 1  # continue on
            else:
                self.logger.critical("Critical error whilst processing, quitting")

        # ok now that the recovered proc_batch has completed, continue on from this proc_batch_id
        proc_batch_id = last_proc_batch_id

        # continue until all is finished or something breaks
        while True:
            # first get the batch from the data provider
            self.logger.debug(
                f"asking DataProvider for next batch: {proc_batch_id} ({self.BATCH_SIZE})"
            )
            status_rows_dp = self.data_provider.get_next_batch(
                proc_batch_id, self.BATCH_SIZE
            )
            if status_rows_dp is None:
                self.logger.debug("No source_batch remaining, all done, quitting...")
                break

            if self._run_proc_batch(status_rows_dp, proc_batch_id) is False:
                self.logger.critical("Critical error whilst processing, quitting")
                break

            # update the proc_batch_id and continue on
            proc_batch_id += 1

    # The proc_batch (list of StatusRow objects) is processed in 5 steps:
    #
    # 1. Register the batch in the ProcessingEnvironment
    # 2. Tell the ProcessingEnvironment to start processing the batch
    # 3. Monitor the ProcessingEnvironment's progress until it's done
    # 4. Retrieve the output from the ProcessingEnvironment
    # 5. Feed the output to the Exporter, so results are put in a happy place
    def _run_proc_batch(
        self, status_rows: List[StatusRow], proc_batch_id: int, skip_steps: int = 0
    ) -> bool:

        if skip_steps >= 5:
            self.logger.warning(
                f"Warning: why are you skipping so many (i.e. {skip_steps}) steps?"
            )
            return True

        if skip_steps == 0:  # first register the batch in the proc env
            if not self._register_proc_batch(proc_batch_id, status_rows):
                return False

        if skip_steps < 2:  # Alright let's ask the proc env to start processing
            if not self._process_proc_batch(proc_batch_id):
                return False

        if skip_steps < 3:  # monitor the processing, until it returns the results
            if not self._monitor_proc_batch(proc_batch_id):
                return False

        if skip_steps < 5:
            # now fetch the results from the ProcessingEnvironment
            # even if this was already done, it's required again for the unfinished export
            processing_results = self._fetch_proc_batch_output(proc_batch_id)

            if processing_results and self._export_proc_batch_output(
                proc_batch_id, processing_results
            ):
                return True
            else:
                return False

        return True

    def _register_proc_batch(
        self, proc_batch_id: int, proc_batch: List[StatusRow]
    ) -> bool:
        self.logger.info(f"Registering batch: {proc_batch_id}")
        status_rows = self.data_processing_env.register_batch(proc_batch_id, proc_batch)
        if status_rows is None:
            self.logger.error(f"Could not register batch {proc_batch_id}, quitting")
            return False
        self.logger.info(f"Successfully registered batch: {proc_batch_id}")
        return True

    def _process_proc_batch(self, proc_batch_id: int) -> bool:
        self.logger.info(f"Triggering proc_batch to start processing: {proc_batch_id}")
        status_rows = self.data_processing_env.process_batch(proc_batch_id)
        if status_rows is None:
            self.logger.error(
                f"Could not trigger proc_batch {proc_batch_id} to start processing, quitting"
            )
            return False
        self.logger.info(f"Successfully triggered the process for: {proc_batch_id}")
        return True

    def _monitor_proc_batch(self, proc_batch_id: int) -> bool:
        self.logger.info(
            f"Start monitoring proc_batch until it finishes: {proc_batch_id}"
        )
        status_rows = self.data_processing_env.monitor_batch(proc_batch_id)
        if status_rows is None:
            self.logger.error(
                f"Error while monitoring proc_batch: {proc_batch_id}, quitting"
            )
            return False
        self.logger.info(
            f"Successfully monitored proc_batch: {proc_batch_id} till it finished"
        )
        return True

    def _fetch_proc_batch_output(
        self, proc_batch_id: int
    ) -> Optional[List[ProcessingResult]]:
        self.logger.info(f"Fetching output data for proc_batch: {proc_batch_id}")
        output = self.data_processing_env.fetch_results_of_batch(proc_batch_id)
        if output is None:
            self.logger.error(
                f"Did not receive any processing results for {proc_batch_id}, quitting"
            )
            return None
        self.logger.info(
            f"Successfully retrieved output for proc_batch {proc_batch_id}"
        )
        return output

    def _export_proc_batch_output(
        self, proc_batch_id: int, processing_results: List[ProcessingResult]
    ) -> bool:
        self.logger.info(f"Exporting proc_batch output: {proc_batch_id}")
        if not self.exporter.export_results(processing_results):
            self.logger.warning(f"Could not export proc_batch {proc_batch_id} output")
            return False

        self.logger.info(f"Successfully exported proc_batch {proc_batch_id} output")
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
