from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum, unique
from typing import List, Optional, Tuple
from pathlib import Path
from dane_workflows.util.base_util import (
    get_logger,
    check_setting,
    load_config,
    validate_file_paths,
)
import sqlite3
from sqlite3 import Error

# from dane_workflows.data_provider import DataProvider

"""
Represents whether the DANE processing of a resource was successful or not
Possibly, other statuses will be added later on
"""


@unique
class ProcessingStatus(IntEnum):
    NEW = 1  # nothing has been done to the item yet

    # states of a batch
    BATCH_ASSIGNED = 2  # the TaskScheduler assigned a proc_batch_id
    BATCH_REGISTERED = 3  # the item was registered in the processing env

    # row-level state
    PROCESSING = 4  # the item is currently processing in the processing env
    PROCESSED = 5  # the item was successfully processed by the processing environment
    EXPORTED = 6  # processing data was reconsiled with source
    ERROR = 7  # the item failed to process properly (proc_error_code will be assigned)
    FINISHED = 8  # the item was successfully processed

    @staticmethod
    def completed_statuses():
        """Returns a list of the statuses we consider as indicating the process is complete"""
        return [ProcessingStatus.ERROR, ProcessingStatus.FINISHED]

    @staticmethod
    def running_statuses():
        """Returns a list of the statuses we consider as indicating the process is still running"""
        return [
            ProcessingStatus.NEW,
            ProcessingStatus.BATCH_ASSIGNED,
            ProcessingStatus.BATCH_REGISTERED,
            ProcessingStatus.PROCESSING,
            ProcessingStatus.PROCESSED,
            ProcessingStatus.EXPORTED,
        ]


@unique
class ErrorCode(IntEnum):  # TODO assign this to each StatusRow
    # batch-level error code
    BATCH_ASSIGN_FAILED = (
        1  # could not assign a proc_batch_id (should hardly ever happen)
    )
    BATCH_REGISTER_FAILED = 2  # the proc env failed to register the batch
    BATCH_PROCESSING_NOT_STARTED = (
        3  # the proc env failed to start processing the registered batch
    )

    # item-level error code
    PROCESSING_FAILED = 4  # the proc env could not process this item
    EXPORT_FAILED_SOURCE_DOC_NOT_FOUND = (
        5  # the doc at the source does not exist (anymore)
    )
    EXPORT_FAILED_SOURCE_DB_CONNECTION_FAILURE = (
        6  # could not connect to source db to export results
    )
    EXPORT_FAILED_PROC_ENV_OUTPUT_UNSUITABLE = (
        7  # the proc env output data is not suitable for export
    )
    IMPOSSIBLE = 8  # this item is impossible to process


@dataclass
class StatusRow:
    target_id: str  # Use this to reconcile results with source catalog (DANE.Document.target.id)
    target_url: str  # So DataProcessingEnvironment can get to the content (DANE.Document.target.url)
    status: ProcessingStatus  # a ProcessingStatus value
    source_batch_id: int  # source_batch_id (automatically incremented)
    source_batch_name: Optional[str]  # also store "semantic" batch ID
    source_extra_info: Optional[
        str
    ]  # allow data providers to store a bit of extra info
    proc_batch_id: Optional[int]  # provided by the TaskScheduler, increments
    proc_id: Optional[str]  # ID assigned by the DataProcessingEnvironment
    proc_status_msg: Optional[
        str
    ]  # Human readable status message from DataProcessingEnvironment
    proc_error_code: Optional[
        ErrorCode
    ]  # in case of status == ERROR, learn more about why

    def __hash__(self):
        return hash(self.target_id)

    def __eq__(self, other):
        return other.target_id == self.target_id


# TODO implement an ExampleStatusHandler as well
# TODO move to status_handler.py (outside of the util package)
class StatusHandler(ABC):
    def __init__(self, config):

        # check if the configured TYPE is the same as the StatusHandler being instantiated
        if self.__class__.__name__ != config["STATUS_HANDLER"]["TYPE"]:
            print("Malconfigured class instance")
            quit()

        # only used so the data provider knows which source_batch it was at
        self.cur_source_batch: List[StatusRow] = None  # call recover to fill it
        self.logger = get_logger(config)
        self.config = (
            config["STATUS_HANDLER"]["CONFIG"]
            if "CONFIG" in config["STATUS_HANDLER"]
            else {}
        )

        # enforce config validation
        if not self._validate_config():
            self.logger.error("Malconfigured, quitting...")
            quit()

    """ ------------------------------------ ABSTRACT FUNCTIONS -------------------------------- """

    @abstractmethod
    def _validate_config(self) -> bool:
        raise NotImplementedError("All DataProviders should implement this")

    # called via recover() on start-up of the TaskScheduler
    @abstractmethod
    def _recover_source_batch(self):
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def _recover_proc_batch(self):
        raise NotImplementedError("Requires implementation")

    # TODO change this function so it just persists all provided status_rows
    @abstractmethod
    def _persist(self, status_rows: List[StatusRow]) -> bool:
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_rows_of_proc_batch(
        self, proc_batch_id: int
    ) -> Optional[List[StatusRow]]:
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_rows_of_source_batch(
        self, source_batch_id: int
    ) -> Optional[List[StatusRow]]:
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_last_proc_batch_id(self) -> int:
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_last_source_batch_id(self) -> int:
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_counts(self) -> Optional[dict]:
        """Counts the number of rows with each status
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_error_code_counts(self) -> Optional[dict]:
        """Counts the number of rows with each error code
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_counts_for_proc_batch_id(self, proc_batch_id: int) -> Optional[dict]:
        """Counts the number of rows with each status for the processing batch
        Args:
            - proc_batch_id - id of the processing batch for which the statuses are counted
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_error_code_counts_for_proc_batch_id(
        self, proc_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each error code for the processing batch
        Args:
            - proc_batch_id - id of the processing batch for which the statuses are counted
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_counts_for_source_batch_id(
        self, source_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each status for the source batch
        Args:
            - source_batch_id - id of the source batch for which the statuses are counted
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_error_code_counts_for_source_batch_id(
        self, source_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each error code for the source batch
        Args:
            - source_batch_id - id of the source batch for which the statuses are counted
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_status_counts_per_extra_info_value(self) -> Optional[dict]:
        """Counts the number of rows with each status for each extra_info value
        Returns:
             - a dict with the various extra_info values as keys, with a dict as value that has
              the various statuses as keys, and the counts of the statuses within that extra_info group as values"""
        raise NotImplementedError("Requires implementation")

    @abstractmethod
    def get_completed_semantic_source_batch_ids(
        self,
    ) -> Tuple[Optional[List[str]], Optional[List[str]]]:
        """Gets lists of the semantic_source_batch_id for all completed
        source batches (where the statuses are only either FINISHED or ERROR), and all uncompleted source batches
        Returns:
            - completed_semantic_source_batch_ids - a list of the semantic_source_batch_ids for the completed batches
            - uncompleted_semantic_source_batch_ids - a list of the semantic_source_batch_ids for the
            uncompleted batches"""
        raise NotImplementedError("Requires implementation")

    """ --------------------- SOURCE BATCH SPECIFIC FUNCTIONS ------------------ """

    def get_current_source_batch(self):
        return self.cur_source_batch

    # called by the data provider to start keeping track of the latest source batch
    def set_current_source_batch(self, status_rows: List[StatusRow]):
        self.logger.debug("Setting new source batch")
        self.logger.debug(status_rows)
        self.cur_source_batch = status_rows  # set the new source batch data
        return self._persist(status_rows)

    # Get a list of IDs for a certain ProcessingStatus
    def get_sb_status_rows_of_type(
        self, proc_status: ProcessingStatus, batch_size: int
    ) -> Optional[List[StatusRow]]:
        status_rows = list(
            filter(lambda x: x.status == proc_status, self.cur_source_batch)
        )
        status_rows = (
            status_rows[0:batch_size] if len(status_rows) >= batch_size else status_rows
        )
        return status_rows if len(status_rows) > 0 else None

    def get_cur_source_batch_id(self) -> int:
        return (
            self.cur_source_batch[0].source_batch_id
            if self.cur_source_batch is not None
            else -1
        )

    """ --------------------- ALL STATUS ROWS FUNCTIONS ------------------ """

    def update_status_rows(
        self,
        status_rows: List[StatusRow],
        status: ProcessingStatus = None,
        proc_batch_id=-1,
        proc_status_msg: str = None,
        proc_error_code: ErrorCode = None,
    ) -> List[StatusRow]:
        for row in status_rows:
            row.status = status if status is not None else row.status
            row.proc_status_msg = (
                proc_status_msg if proc_status_msg is not None else row.proc_status_msg
            )
            if proc_batch_id != -1:
                row.proc_batch_id = proc_batch_id
            if proc_error_code is not None:
                row.proc_error_code = proc_error_code
        return status_rows

    # TODO make sure that this function can save regardless of source_batch_id!
    def persist(self, status_rows: List[StatusRow]) -> bool:
        if not status_rows or type(status_rows) != list:
            self.logger.warning(
                "Warning: trying to update status with invalid/empty status data"
            )
            return False

        if self._persist(status_rows):
            self.logger.debug(
                "persisted updated status_rows, now syncing with current source batch"
            )
            return (
                self._recover_source_batch()
            )  # make sure the source batch is also updated
        self.logger.error("Could not persist status rows!")
        return False

    def recover(
        self,
    ) -> Tuple[bool, Optional[List[StatusRow]]]:  # returns StatusRows of proc_batch
        source_batch_recovered = self._recover_source_batch()
        if source_batch_recovered is False:
            self.logger.warning("Could not recover any source batch")

        cur_proc_batch = self._recover_proc_batch()
        if cur_proc_batch is None:
            self.logger.warning("Could not recover proc batch")
        return (
            source_batch_recovered,
            cur_proc_batch,
        )  # TaskScheduler should sync this with the proc env last status


class ExampleStatusHandler(StatusHandler):
    def __init__(self, config):
        super().__init__(config)

    def _validate_config(self) -> bool:
        self.logger.debug(f"Validating {self.__class__.__name__} config")
        return True  # no particular settings for this StatusHandler

    # called on start-up of the TaskScheduler
    def _recover_source_batch(self) -> bool:
        self.logger.debug(f"{self.__class__.__name__} cannot recover any status")
        self.cur_source_batch: List[StatusRow] = []
        return False  # in memory only, so cannot recover

    # called on start-up of the TaskScheduler
    def _recover_proc_batch(self) -> bool:
        self.logger.debug(f"{self.__class__.__name__} cannot recover any status")
        return False  # in memory only, so cannot recover

    def _persist(self, status_rows: List[StatusRow]) -> bool:
        return True  # does nothing, returns True to satisfy set_current_source_batch

    def get_status_rows_of_proc_batch(
        self, proc_batch_id: int
    ) -> Optional[List[StatusRow]]:
        return None  # TODO implement

    def get_status_rows_of_source_batch(
        self, source_batch_id: int
    ) -> Optional[List[StatusRow]]:
        return None  # TODO implement

    def get_last_proc_batch_id(self) -> int:
        return -1  # TODO implement

    def get_last_source_batch_id(self) -> int:
        return -1  # TODO implement

    def get_status_counts(self) -> dict:
        return {}  # TODO implement

    def get_error_code_counts(self) -> dict:
        return {}  # TODO implement

    def get_status_counts_for_proc_batch_id(self, proc_batch_id: int) -> dict:
        return {}  # TODO implement

    def get_error_code_counts_for_proc_batch_id(self, proc_batch_id: int) -> dict:
        return {}  # TODO implement

    def get_status_counts_for_source_batch_id(self, source_batch_id: int) -> dict:
        return {}  # TODO implement

    def get_error_code_counts_for_source_batch_id(self, source_batch_id: int) -> dict:
        return {}  # TODO implement

    def get_status_counts_per_extra_info_value(self) -> dict:
        return {}  # TODO implement

    def get_completed_semantic_source_batch_ids(
        self,
    ) -> Tuple[Optional[List[str]], Optional[List[str]]]:
        return ([], [])  # TODO implement


class SQLiteStatusHandler(StatusHandler):
    def __init__(self, config):
        super().__init__(config)
        self.DB_FILE: str = self.config["DB_FILE"]
        if self._init_database() is False:
            self.logger.debug(f"Could not initialize the DB: {self.DB_FILE}")
            quit()

    def _init_database(self):
        conn = self._create_connection(self.DB_FILE)
        if conn is None:
            return False
        with conn:
            return self._create_table(conn, self._get_table_sql())
        return False

    def _validate_config(self) -> bool:
        self.logger.debug(f"Validating {self.__class__.__name__} config")
        try:
            assert "DB_FILE" in self.config, "SQLiteStatusHandler config incomplete"
            assert check_setting(
                self.config["DB_FILE"], str
            ), "SQLiteStatusHandler.DB_FILE"
            validate_file_paths(
                [Path(self.config["DB_FILE"]).parent]
            )  # parent dir must exist
        except AssertionError as e:
            self.logger.error(f"Configuration error: {str(e)}")
            return False

        return True

    # called on start-up of the TaskScheduler
    def _recover_source_batch(self) -> bool:
        self.logger.debug("Recovering source batch")
        source_batch_id = self.get_last_source_batch_id()
        if source_batch_id == -1:
            self.logger.info("no source batch ID found in DB, nothing to recover")
            return False
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT * FROM status_rows WHERE source_batch_id=?",
                (source_batch_id,),
            )
            if db_rows:
                self.logger.info("Recovered a source batch from the DB")
                self.cur_source_batch = self._to_status_rows(db_rows)
                return True
        self.logger.info("Could not recover a source batch somehow")
        return False

    # called on start-up of the TaskScheduler
    def _recover_proc_batch(self) -> bool:
        self.logger.debug(f"{self.__class__.__name__} cannot recover any status")
        return False  # TODO

    def _persist(self, status_rows: List[StatusRow]) -> bool:
        conn = self._create_connection(self.DB_FILE)
        with conn:
            for row in status_rows:
                self._insert_or_replace_status_row(conn, self._to_tuple(row))
            return True
        return False

    def get_status_rows_of_proc_batch(
        self, proc_batch_id: int
    ) -> Optional[List[StatusRow]]:
        self.logger.debug("Fetching proc batch from DB")
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT * FROM status_rows WHERE proc_batch_id=?",
                (proc_batch_id,),
            )
            if db_rows:
                return self._to_status_rows(db_rows)
        return None

    def get_status_rows_of_source_batch(
        self, source_batch_id: int
    ) -> Optional[List[StatusRow]]:
        self.logger.debug("Fetching source batch from DB")
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT * FROM status_rows WHERE source_batch_id=?",
                (source_batch_id,),
            )
            if db_rows:
                return self._to_status_rows(db_rows)
        return None

    def get_last_proc_batch_id(self) -> int:
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn, "SELECT MAX(proc_batch_id) FROM status_rows", ()
            )
            return self._get_single_int_from_db_rows(db_rows)
        return -1

    def get_last_source_batch_id(self) -> int:
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn, "SELECT MAX(source_batch_id) FROM status_rows", ()
            )
            return self._get_single_int_from_db_rows(db_rows)
        return -1

    def get_status_counts(self) -> Optional[dict]:
        """Counts the number of rows with each status
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT status, count(status) FROM status_rows " "GROUP BY status",
                (),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_error_code_counts(self) -> Optional[dict]:
        """Counts the number of rows with each error code
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT proc_error_code, count(proc_error_code) FROM status_rows "
                "GROUP BY proc_error_code",
                (),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_status_counts_for_proc_batch_id(self, proc_batch_id: int) -> Optional[dict]:
        """Counts the number of rows with each status for the processing batch
        Args:
            - proc_batch_id - id of the processing batch for which the statuses are counted
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT status, count(status) FROM status_rows WHERE proc_batch_id = ? "
                "GROUP BY status",
                (proc_batch_id,),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_error_code_counts_for_proc_batch_id(
        self, proc_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each error code for the processing batch
        Args:
            - proc_batch_id - id of the processing batch for which the statuses are counted
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT proc_error_code, count(proc_error_code) FROM status_rows "
                "WHERE proc_batch_id = ? "
                "GROUP BY proc_error_code",
                (proc_batch_id,),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_status_counts_for_source_batch_id(
        self, source_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each status for the source batch
        Args:
            - source_batch_id - id of the source batch for which the statuses are counted
        Returns:
             - a dict with the various statuses as keys, and the counts of the statuses
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT status, count(status) FROM status_rows WHERE source_batch_id = ? "
                "GROUP BY status",
                (source_batch_id,),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_error_code_counts_for_source_batch_id(
        self, source_batch_id: int
    ) -> Optional[dict]:
        """Counts the number of rows with each error code for the source batch
        Args:
            - source_batch_id - id of the source batch for which the statuses are counted
        Returns:
             - a dict with the various error codes as keys, and the counts of the error codes
                as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT proc_error_code, count(proc_error_code) FROM status_rows "
                "WHERE source_batch_id = ? "
                "GROUP BY proc_error_code",
                (source_batch_id,),
            )
            return self._get_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_status_counts_per_extra_info_value(self) -> Optional[dict]:
        """Counts the number of rows with each status for each extra_info value
        Returns:
             - a dict with the various extra_info values as keys, with a dict as value that has
              the various statuses as keys, and the counts of the statuses within that extra_info group as values"""
        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT source_extra_info, status, COUNT(status) FROM status_rows "
                "GROUP BY source_extra_info, status",
                (),
            )
            return self._get_nested_groups_and_counts_from_db_rows(db_rows)
        return None

    def get_completed_semantic_source_batch_ids(
        self,
    ) -> Tuple[Optional[List[str]], Optional[List[str]]]:
        """Gets lists of the semantic_source_batch_id for all completed
        source batches (where the statuses are only either FINISHED or ERROR), and all uncompleted source batches
        Returns:
            - completed_semantic_source_batch_ids - a list of the semantic_source_batch_ids for the completed batches
            - uncompleted_semantic_source_batch_ids - a list of the semantic_source_batch_ids for the
            uncompleted batches"""
        completed_semantic_source_batch_ids = []
        uncompleted_semantic_source_batch_ids = []

        conn = self._create_connection(self.DB_FILE)
        with conn:
            db_rows = self._run_select_query(
                conn,
                "SELECT source_batch_name, GROUP_CONCAT(status) FROM status_rows "
                "GROUP BY source_batch_name",
                (),
            )
            statuses_per_batch = self._get_groups_and_counts_from_db_rows(db_rows)

        if statuses_per_batch:

            for semantic_source_batch_id in statuses_per_batch:
                if any(
                    str(int(running_status))
                    in str(statuses_per_batch[semantic_source_batch_id])
                    for running_status in ProcessingStatus.running_statuses()
                ):
                    uncompleted_semantic_source_batch_ids.append(
                        semantic_source_batch_id
                    )
                else:
                    completed_semantic_source_batch_ids.append(semantic_source_batch_id)

            return (
                completed_semantic_source_batch_ids,
                uncompleted_semantic_source_batch_ids,
            )

        return (None, None)

    def _get_single_int_from_db_rows(self, db_rows):
        if db_rows and type(db_rows) == list and len(db_rows) == 1:
            t_value = db_rows[0]
            return t_value[0] if t_value[0] is not None else -1
        return -1

    def _get_groups_and_counts_from_db_rows(self, db_rows) -> Optional[dict]:
        """Processes the results of an aggregation for a group, e.g. COUNT and GROUP BY, to retrieve a single
        aggregated value for each group
        Returns:
            - a dict with the groups as keys and their aggregated values as values
        """
        group_counts = {}
        if db_rows and type(db_rows) == list and len(db_rows) > 0:
            for db_row in db_rows:
                group_counts[db_row[0]] = db_row[1]
            return group_counts
        else:
            return None

    def _get_nested_groups_and_counts_from_db_rows(self, db_rows) -> Optional[dict]:
        """Processes the results of a nested aggregation for a nested group with 2 levels,
        e.g. COUNT and GROUP BY x, y, to retrieve a single
        aggregated value for each nested group
        Returns:
            - a dict with the first groups as keys and a dict as value,
            that contains the second groups as keys and their aggregated values as values
        """
        group_counts: dict = {}
        if db_rows and type(db_rows) == list and len(db_rows) > 0:
            for db_row in db_rows:
                if db_row[0] not in group_counts:
                    group_counts[db_row[0]] = {}
                group_counts[db_row[0]][db_row[1]] = db_row[2]
            return group_counts
        else:
            return None

    """ ----------------------- SQLLITE SPECIFIC FUNCTIONS -------------------------- """

    def _create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error:
            self.logger.exception(f"Could not connect to DB: {db_file}")
        return conn

    def _create_table(self, conn, create_table_sql) -> bool:
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
            return True
        except Error:
            self.logger.exception("Could not create status_rows table")
        return False

    def _delete_all_rows(self):
        try:
            conn = self._create_connection(self.DB_FILE)
            conn.execute("DELETE FROM status_rows")
            conn.commit()
            return True
        except Error:
            self.logger.exception("Could not delete all status_rows from table")
        return False

    def _get_table_sql(self):
        return """CREATE TABLE IF NOT EXISTS status_rows (
            target_id text NOT NULL,
            target_url text NOT NULL,
            status integer NOT NULL,
            source_batch_id integer NOT NULL,
            source_batch_name text,
            source_extra_info text,
            proc_batch_id integer,
            proc_id integer,
            proc_status_msg text,
            proc_error_code integer,
            PRIMARY KEY (target_id, target_url)
        );"""

    def _left_shift_tuple(self, tup, n):
        try:
            n = n % len(tup)
        except ZeroDivisionError:
            return tuple()
        return tup[n:] + tup[0:n]

    def _to_tuple(self, row: StatusRow):
        t = (
            row.target_id,
            row.target_url,
            row.status.value if row.status is not None else None,
            row.source_batch_id,
            row.source_batch_name,
            row.source_extra_info,
            row.proc_batch_id,
            row.proc_id,
            row.proc_status_msg,
            row.proc_error_code.value if row.proc_error_code is not None else None,
        )
        return t

    def _to_status_rows(self, db_rows) -> List[StatusRow]:
        return [
            StatusRow(
                row[0],
                row[1],
                ProcessingStatus(row[2]),  # should always be filled
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                ErrorCode(row[9]) if row[9] else None,
            )
            for row in db_rows
        ]

    def _create_status_row(self, conn, row_tuple) -> int:
        self.logger.debug(row_tuple)
        sql = """
            INSERT INTO status_rows(
                target_id,
                target_url,
                status,
                source_batch_id,
                source_batch_name,
                source_extra_info,
                proc_batch_id,
                proc_id,
                proc_status_msg,
                proc_error_code
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """
        cur = conn.cursor()
        cur.execute(sql, row_tuple)
        conn.commit()
        return cur.lastrowid

    def _insert_or_replace_status_row(self, conn, row_tuple):
        self.logger.info("Creating/updating status row")
        self.logger.debug(row_tuple)
        sql = """
            INSERT OR REPLACE INTO status_rows(
                target_id,
                target_url,
                status,
                source_batch_id,
                source_batch_name,
                source_extra_info,
                proc_batch_id,
                proc_id,
                proc_status_msg,
                proc_error_code
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """
        cur = conn.cursor()
        cur.execute(sql, row_tuple)
        conn.commit()
        return cur.lastrowid

    def _run_select_query(self, conn, query, params):
        self.logger.debug(query)
        self.logger.debug(params)
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        return rows


# test your StatusHandler in isolation
if __name__ == "__main__":

    config = load_config("../config-example.yml")
    status_handler = SQLiteStatusHandler(config)