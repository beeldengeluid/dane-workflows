import sys
from abc import ABC, abstractmethod
from typing import List

from dane_workflows.data_processing import ProcessingResult
from dane_workflows.util.base_util import get_logger
from dane_workflows.status import StatusHandler, ProcessingStatus

"""
This class is owned by a TaskScheduler to export results obtained from a processing environment (such as DANE)
"""


class Exporter(ABC):
    def __init__(self, config, status_handler: StatusHandler, unit_test: bool = False):

        # check if the configured TYPE is the same as the Exporter being instantiated
        if self.__class__.__name__ != config["EXPORTER"]["TYPE"]:
            print(f"Malconfigured class instance: {config['EXPORTER']['TYPE']}")
            sys.exit()

        self.config = (
            config["EXPORTER"]["CONFIG"] if "CONFIG" in config["EXPORTER"] else {}
        )

        self.logger = get_logger(config)  # logging was already initialised by owner

        # enforce config validation
        if not self._validate_config():
            self.logger.error("Malconfigured, quitting...")
            sys.exit()

        self.status_handler = status_handler

    @abstractmethod
    def _validate_config(self) -> bool:
        raise NotImplementedError("Implement to validate the config")

    @abstractmethod
    def export_results(self, results: List[ProcessingResult]) -> bool:
        raise NotImplementedError("Implement to export results")


class ExampleExporter(Exporter):
    def __init__(self, config, status_handler: StatusHandler, unit_test: bool = False):
        super().__init__(config, status_handler, unit_test)

    def _validate_config(self) -> bool:
        return True

    def export_results(self, results: List[ProcessingResult]) -> bool:
        if not results:
            self.logger.warning("Received no results for export")
            return False
        self.logger.debug(f"Received {len(results)} results to be exported")
        status_rows = [result.status_row for result in results]
        self.logger.debug("grab status rows from results")
        self.logger.debug(status_rows)
        self.status_handler.persist(  # everything is exported properly
            self.status_handler.update_status_rows(
                status_rows, status=ProcessingStatus.FINISHED
            )
        )
        return True
