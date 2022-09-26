import sys
from abc import ABC, abstractmethod
from typing import List
import logging
from dane_workflows.data_processing import ProcessingResult
from dane_workflows.status import StatusHandler, ProcessingStatus

"""
This class is owned by a TaskScheduler to export results obtained from a processing environment (such as DANE)
"""


logger = logging.getLogger(__name__)


class Exporter(ABC):
    def __init__(self, config, status_handler: StatusHandler, unit_test: bool = False):

        self.config = (
            config["EXPORTER"]["CONFIG"] if "CONFIG" in config["EXPORTER"] else {}
        )

        # enforce config validation
        if not self._validate_config():
            logger.critical("Malconfigured, quitting...")
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
            logger.warning("Received no results for export")
            return False
        logger.info(f"Received {len(results)} results to be exported")
        status_rows = [result.status_row for result in results]
        logger.info("Status rows taken from results:")
        logger.info(status_rows)
        self.status_handler.persist(  # everything is exported properly
            self.status_handler.update_status_rows(
                status_rows, status=ProcessingStatus.FINISHED
            )
        )
        return True
