import sys
from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import uuid4
from dane_workflows.util.base_util import (
    get_logger,
    check_setting,
    load_config,
)
from dane_workflows.status import StatusHandler, StatusRow, ProcessingStatus

"""
This class is owned by a TaskScheduler, which expects this class to provide the next n DAEN Documents
to batch upload to its DANEEnvironment.

The DataProvider also keeps a cache (persisted to disk) to be able to recover which IDs
were already processed AND to keep track of which IDs could not be processed by DANE.
"""


class DataProvider(ABC):
    def __init__(
        self, config: dict, status_handler: StatusHandler, unit_test: bool = False
    ):
        # check if the configured TYPE is the same as the DataProvider being instantiated
        if self.__class__.__name__ != config["DATA_PROVIDER"]["TYPE"]:
            print("Malconfigured class instance")
            sys.exit()

        self.config = config["DATA_PROVIDER"]["CONFIG"]
        self.logger = get_logger(config)  # logging was already initialised by owner
        self.logger.debug("intialising DATA PROVIDER")
        self.status_handler = status_handler

        # enforce config validation
        if not self._validate_config():
            self.logger.error("Malconfigured, quitting...")
            sys.exit()

    """
    ------------------------------ ABSTRACT METHODS --------------------
    """

    @abstractmethod
    def _validate_config(self) -> bool:
        raise NotImplementedError("All DataProviders should implement this")

    @abstractmethod
    def _to_semantic_source_batch_id(self, source_batch_id: int) -> str:
        return str(source_batch_id)  # by default just use the source_batch_id

    @abstractmethod
    def fetch_source_batch_data(
        self, source_batch_id: int
    ) -> Optional[List[StatusRow]]:
        raise NotImplementedError("All DataProviders should implement this")

    """
    ------------------------------ PUBLIC CLASS METHODS --------------------
    """

    # Should return a list of StatusRows for the task scheduler
    def get_next_batch(
        self, proc_batch_id: int, batch_size: int, called_recursively: bool = False
    ) -> Optional[List[StatusRow]]:
        if self.status_handler.get_current_source_batch() is None:
            return None  # means the last batch was delivered

        # 1. get unprocessed from the current source batch
        unprocessed = self.status_handler.get_sb_status_rows_of_type(
            ProcessingStatus.NEW, batch_size
        )

        # 2. if it's empty fetch the next source batch
        if unprocessed is None:
            new_source_batch = self.fetch_source_batch_data(
                self.status_handler.get_cur_source_batch_id() + 1
            )
            self.logger.info(f"New source_batch is ok: {new_source_batch is not None}")
            if new_source_batch:  # make the StatusHandler track the new batch
                if called_recursively:
                    # we have a problem, as we are in an infinite loop
                    self.logger.error(
                        "Entering infinite loop in get_next_batch(), breaking out"
                    )
                    return None
                self.status_handler.set_current_source_batch(new_source_batch)
                self.logger.info(
                    "Loaded new source_batch in memory, now fetching the first proc_batch"
                )
                return self.get_next_batch(
                    proc_batch_id, batch_size, called_recursively=True
                )
            else:  # no more data available
                self.logger.info(
                    "No more data available from source, TaskScheduler should quit"
                )
                return None

        self.logger.debug(f"Continuing with {len(unprocessed)} unprocessed items")

        # 3. assign the proc_batch_id to the unprocessed[0:batch_size]
        self.status_handler.persist(
            self.status_handler.update_status_rows(
                unprocessed,
                status=ProcessingStatus.BATCH_ASSIGNED,
                proc_batch_id=proc_batch_id,
            )
        )

        # 4. just return the selected unprocessed status_rows
        return unprocessed


class ExampleDataProvider(DataProvider):
    def __init__(self, config, status_handler: StatusHandler, unit_test: bool = False):
        super(ExampleDataProvider, self).__init__(config, status_handler, unit_test)

        # either set dummy data OR data provided via self.config["DATA"]
        self.data = [{"id": str(uuid4()), "url": f"https://{x}"} for x in range(0, 100)]
        self.logger.debug(self.config.get("DATA", None))
        if self.config.get("DATA", None) is not None:
            self.logger.info(f"Setting {len(self.config['DATA'])} of custom items")
            self.data = self.config["DATA"]
        else:
            self.logger.info(
                "No DATA specfied in config, continuing with 100 dummy items"
            )

        # now that the config has been validated, assign the config to global vars (for readability)
        self.SOURCE_BATCH_SIZE: int = self.config["SOURCE_BATCH_SIZE"]

    def _validate_config(self) -> bool:
        self.logger.debug(f"Validating {self.__class__.__name__} config")
        try:
            assert (
                "SOURCE_BATCH_SIZE" in self.config
            ), "ExampleDataProvider config incomplete"
            assert check_setting(
                self.config["SOURCE_BATCH_SIZE"], int
            ), "ExampleDataProvider.SOURCE_BATCH_SIZE"
            assert check_setting(
                self.config.get("DATA", None), list, True
            ), "ExampleDataProvider.DATA"
        except AssertionError as e:
            self.logger.error(f"Configuration error: {str(e)}")
            return False

        return True

    # override: how to represent the SourceBatchID for this data provider
    def _to_semantic_source_batch_id(self, source_batch_id: int) -> str:
        return f"Ex__{source_batch_id}"  # just an example to distinguish with source_batch_id

    # override: return all the data needed for source batch (based on the source_batch_id)
    def fetch_source_batch_data(
        self, source_batch_id: int
    ) -> Optional[List[StatusRow]]:
        batch_data = []
        num_available = len(self.data)  # e.g. 52
        offset = source_batch_id * self.SOURCE_BATCH_SIZE
        if offset >= num_available:
            return None

        num_items_in_batch = self.SOURCE_BATCH_SIZE  # e.g. 50
        if offset + self.SOURCE_BATCH_SIZE >= num_available:  # 0 + 50 < 52
            num_items_in_batch = num_available - offset  # 52 - 50 = 2

        for x in range(offset, offset + num_items_in_batch):
            item = self.data[x]
            batch_data.append(
                StatusRow(
                    target_id=item["id"],
                    target_url=item["url"],
                    status=ProcessingStatus.NEW,  # always start with status "NEW"
                    source_batch_id=source_batch_id,
                    source_batch_name=self._to_semantic_source_batch_id(
                        source_batch_id
                    ),
                    source_extra_info="something row-specific",
                    proc_batch_id=None,  # will be designated once the TaskScheduler calls get_next_batch()
                    proc_id=None,  # will be assigned once the item is registered within the ProcessingEnvironment
                    proc_status_msg=None,
                    proc_error_code=None,
                )
            )
        self.logger.debug("fetched source batch data")
        # self.logger.debug(batch_data)
        return batch_data


# Test your DataProvider in isolation
if __name__ == "__main__":
    from dane_workflows.status import SQLiteStatusHandler

    config = load_config("../config-example.yml")
    status_handler = SQLiteStatusHandler(config)
    dp = ExampleDataProvider(config, status_handler)
