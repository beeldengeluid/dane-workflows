import os
import sys
import json
import requests
import logging
from time import sleep, perf_counter
from enum import Enum, IntEnum, unique
from dataclasses import dataclass
from typing import List, Optional, Tuple
from elasticsearch7 import Elasticsearch
from dane import Document
from dane_workflows.status import StatusRow, ProcessingStatus
from dane_workflows.util.dane_query_util import (
    tasks_of_batch_query,
    results_of_batch_query,
    result_of_target_id_query,
    task_of_target_id_query,
)

logger = logging.getLogger(__name__)


@unique
class DANEBatchState(Enum):
    SUCCESS = "success"  # in DANE API response contains list of successes
    FAILED = "failed"  # in DANE API response contains list of failures


@unique
class TaskType(Enum):
    NER = "NER"  # Named Entity Recognition
    ASR = "ASR"  # Automatic Speech Recognition
    DOWNLOAD = "DOWNLOAD"  # Download
    BG_DOWNLOAD = "BG_DOWNLOAD"  # Download via B&G playout-proxy
    FINGERPRINT = "FINGERPRINT"  # Fingerprint extraction


@unique
class TaskState(IntEnum):
    QUEUED = 102  # Task has been sent to a queue, it might be being worked on or held in queue.
    SUCCESS = 200  # Task completed successfully.
    CREATED = 201  # Task is registered, but has not been acted upon.
    TASK_RESET = 205  # Task reset state, typically after manual intervention
    BAD_REQUEST = 400  # Malformed request, typically the document or task description.
    ACCESS_DENIED = 403  # Access denied to underlying source material.
    NOT_FOUND = 404  # Underlying source material not found.
    UNFINISHED_DEPENDENCY = 412  # Task has a dependency which has not completed yet.
    NO_ROUTE_TO_QUEUE = (
        422  # If a task cannot be routed to a queue, this state is returned.
    )
    ERROR = 500  # Error occurred during processing, details should be given in message.
    ERROR_INVALID_INPUT = 502  # Worker received invalid or partial input.
    ERROR_PROXY = (
        503  # Worker received an error response from a remote service it depends on.
    )


@dataclass
class Task:
    id: str  # es_hit["_id"],
    message: str  # es_hit["_source"]["task"]["msg"],
    state: int  # int(es_hit["_source"]["task"]["state"]),
    priority: int  # int(es_hit["_source"]["task"]["priority"]),
    key: str  # es_hit["_source"]["task"]["key"],
    created_at: str  # es_hit["_source"]["created_at"],
    updated_at: str  # es_hit["_source"]["updated_at"],
    doc_id: str  # es_hit["_source"]["role"]["parent"]


@dataclass
class Result:
    id: str  # es_hit["_id"]
    generator: dict  # es_hit["_source"]["result"]["generator"]
    payload: dict  # es_hit["_source"]["result"]["payload"]
    created_at: str  # es_hit["_source"]["created_at"],
    updated_at: str  # es_hit["_source"]["updated_at"],
    task_id: str  # es_hit["_source"]["role"]["parent"]
    doc_id: Optional[str]  # TODO not sure yet how to fetch this
    # provenance: Optional[dict] TODO fill this in _to_result()


class DANEHandler:
    def __init__(self, config: dict):

        # TODO validate_config
        self.DANE_TASK_ID = config["DANE_TASK_ID"]
        self.DANE_HOST = config["DANE_HOST"]

        self.DANE_API = f"http://{self.DANE_HOST}/api"
        self.DANE_UI = (
            f"http://{self.DANE_HOST}/manage"  # only for friendly debug messages
        )

        self.DANE_DOC_ENDPOINT = f"http://{self.DANE_HOST}/DANE/document/"
        self.DANE_DOCS_ENDPOINT = f"http://{self.DANE_HOST}/DANE/documents/"
        self.DANE_TASK_ENDPOINT = f"http://{self.DANE_HOST}/DANE/task/"

        self.STATUS_DIR = config["DANE_STATUS_DIR"]
        self.MONITOR_INTERVAL = config["DANE_MONITOR_INTERVAL"]

        self.BATCH_PREFIX = config["DANE_BATCH_PREFIX"]

        # TODO implement new endpoint in DANE-server API to avoid calling ES directly
        self.DANE_ES = Elasticsearch(
            host=config["DANE_ES_HOST"],
            port=config["DANE_ES_PORT"],
        )
        self.DANE_ES_INDEX = config["DANE_ES_INDEX"]
        self.DANE_ES_QUERY_TIMEOUT = config["DANE_ES_QUERY_TIMEOUT"]

    def _get_batch_file_name(self, proc_batch_id: int) -> str:
        fn = os.path.join(
            self.STATUS_DIR, f"{self._get_proc_batch_name(proc_batch_id)}.json"
        )
        logger.info(f"{proc_batch_id} --> filename: {fn}")
        return fn

    def _load_batch_file(self, proc_batch_id) -> Optional[dict]:
        logger.info("Entering function")
        try:
            return json.load(open(self._get_batch_file_name(proc_batch_id)))
        except Exception:
            logger.exception(
                f"Could not load {self._get_proc_batch_name(proc_batch_id)}.json"
            )
            return None

    # use to feed _add_tasks_to_batch()
    def _get_doc_ids_of_batch(self, proc_batch_id: int) -> Optional[List[str]]:
        logger.info(
            f"Get DANE doc IDs for proc_batch: {self._get_proc_batch_name(proc_batch_id)}"
        )
        batch_data = self._load_batch_file(proc_batch_id)
        if batch_data is None:
            return None

        # extract al docs (failed/success) from the persisted proc_batch file
        dane_docs = self.__extract_docs_by_state(batch_data, DANEBatchState.SUCCESS)
        dane_docs.extend(
            self.__extract_docs_by_state(batch_data, DANEBatchState.FAILED)
        )

        # return the ids only
        return [doc._id for doc in dane_docs] if len(dane_docs) > 0 else None

    """
    ------------------------------- DANE API CALLS ---------------------------
    [
        {
            "_id": "780b9753513ff924c64bae43fcffb9646797ab28",
            "key": "ASR",
            "state": "412",
            "msg": "Unfinished dependencies",
            "priority": 1,
            "created_at": "2022-10-26T10:26:35",
            "updated_at": "2022-10-26T10:26:35",
            "args": {
            "*": null
            }
        },
        {
            "_id": "52e21b867622e857135a67561f0664cabfea967a",
            "key": "BG_DOWNLOAD",
            "state": "500",
            "msg": "Failed to receive a file from the B&G proxy",
            "priority": 1,
            "created_at": "2022-10-26T10:26:35",
            "updated_at": "2022-10-27T07:34:47",
            "args": {
            "*": null
            }
        }
        ]
    """

    def _get_tasks_of_document(
        self, doc_id: str, leaf_task_to_omit: str = None
    ) -> List[Task]:
        logger.info(
            f"Fetching tasks of document {doc_id}, filtering out {leaf_task_to_omit}"
        )
        try:
            resp = requests.get(f"{self.DANE_DOC_ENDPOINT}/{doc_id}/tasks")
            if resp.status_code != 200:
                logger.error(
                    f"Failed to fetch tasks for {doc_id}; status_code={resp.status_code}"
                )
                logger.error(resp.text)
                return []

            # returns list of Task objects TODO use DANE.Task later on
            task_data = json.loads(resp.text)
            return list(
                filter(
                    lambda x: x.key != leaf_task_to_omit,
                    [
                        Task(
                            t["_id"],
                            t["msg"],
                            int(t["state"]),
                            int(t["priority"]),
                            t["key"],
                            t["created_at"],
                            t["updated_at"],
                            doc_id,
                        )
                        for t in task_data
                    ],
                )
            )
        except json.JSONDecodeError:
            logger.exception("No JSON data returned by DANE API")
            return []
        except Exception:
            logger.exception(f"Failed to fetch tasks for doc ID: {doc_id}")
            return []

    """
    ------------------------------ ES FUNCTIONS (UNDESIRABLE, BUT REQUIRED) ----------------
    """

    # TODO this function needs to be put in the DANE API!
    def get_tasks_of_batch(
        self, proc_batch_id: int, all_tasks: List[Task], offset=0, size=200
    ) -> List[Task]:
        logger.info(
            f"Fetching tasks of proc_batch: {self._get_proc_batch_name(proc_batch_id)} from DANE index"
        )
        query = tasks_of_batch_query(
            self._get_proc_batch_name(proc_batch_id), offset, size, self.DANE_TASK_ID
        )
        logger.debug(json.dumps(query, indent=4, sort_keys=True))
        result = self.DANE_ES.search(
            index=self.DANE_ES_INDEX,
            body=query,
            request_timeout=self.DANE_ES_QUERY_TIMEOUT,  # timeout reached! (60 seconds)
        )  # TODO better exception handling (OR fix by moving this to DANE-serve API)
        if len(result["hits"]["hits"]) <= 0:
            return all_tasks
        else:
            for hit in result["hits"]["hits"]:
                all_tasks.append(self._to_task(hit))
            logger.info(
                f"Done fetching all tasks for batch {self._get_proc_batch_name(proc_batch_id)}"
            )
            return self.get_tasks_of_batch(
                proc_batch_id, all_tasks, offset + size, size
            )

    def get_results_of_batch(
        self, proc_batch_id: int, all_results: List[Result], offset=0, size=200
    ) -> List[Result]:
        logger.info("Entering function")
        logger.info(
            f"Fetching results of proc_batch: {self._get_proc_batch_name(proc_batch_id)} from DANE index"
        )
        query = results_of_batch_query(
            self._get_proc_batch_name(proc_batch_id), offset, size, self.DANE_TASK_ID
        )
        logger.debug(json.dumps(query, indent=4, sort_keys=True))
        result = self.DANE_ES.search(
            index=self.DANE_ES_INDEX,
            body=query,
            request_timeout=self.DANE_ES_QUERY_TIMEOUT,
        )
        if len(result["hits"]["hits"]) <= 0:
            return all_results
        else:
            for hit in result["hits"]["hits"]:
                all_results.append(self._to_result(hit))
            logger.info(
                f"Done fetching all results for batch {self._get_proc_batch_name(proc_batch_id)}"
            )
            return self.get_results_of_batch(
                proc_batch_id, all_results, offset + size, size
            )

    def get_result_of_target_id(self, target_id: str):
        logger.info(f"Getting result of target_id {target_id}")
        query = result_of_target_id_query(target_id, self.DANE_TASK_ID)

        result = self.DANE_ES.search(
            index=self.DANE_ES_INDEX,
            body=query,
            request_timeout=self.DANE_ES_QUERY_TIMEOUT,
        )
        logger.info(f"Found: {result['hits']['total']['value']} results")
        if result["hits"]["total"]["value"] == 1:
            data = result["hits"]["hits"][0]
            logger.debug(data)
            return self._to_result(data)

        logger.warning(f"No result found for target_id: {target_id}")
        return None

    def get_task_of_target_id(self, target_id: str):
        logger.info(f"Getting task of target_id {target_id}")
        query = task_of_target_id_query(target_id, self.DANE_TASK_ID)

        result = self.DANE_ES.search(
            index=self.DANE_ES_INDEX,
            body=query,
            request_timeout=self.DANE_ES_QUERY_TIMEOUT,
        )
        logger.info(f"Found: {result['hits']['total']['value']} tasks")
        if result["hits"]["total"]["value"] == 1:
            data = result["hits"]["hits"][0]
            logger.debug(data)
            return self._to_task(data)

        logger.warning(f"No task found for target_id: {target_id}")
        return None

    # TODO check out if DANE.TASK.from_json also works well instead of this dataclass
    def _to_task(self, es_hit: dict) -> Task:
        logger.info("Entering function")
        return Task(
            es_hit["_id"],
            es_hit["_source"]["task"]["msg"],
            int(es_hit["_source"]["task"]["state"]),
            int(es_hit["_source"]["task"]["priority"]),
            es_hit["_source"]["task"]["key"],
            es_hit["_source"]["created_at"],
            es_hit["_source"]["updated_at"],
            es_hit["_source"]["role"]["parent"],  # refers to the DANE.Document._id
        )

    # TODO check out if DANE.TASK.from_json also works well instead of this dataclass
    def _to_result(self, es_hit: dict) -> Result:
        logger.info("Entering function")
        return Result(
            es_hit["_id"],
            es_hit["_source"]["result"]["generator"],
            es_hit["_source"]["result"]["payload"],
            es_hit["_source"]["created_at"],
            es_hit["_source"]["updated_at"],
            es_hit["_source"]["role"]["parent"],  # refers to the DANE.Task._id
            None,  # NOTE DANE.Document._id is added later when merging with DANE.Tasks
            # None, TODO provenance will be added later (not implemented yet)
        )

    """
    -------------------------------- PUBLIC FUNCTIONS ---------------------
    """

    def register_batch(
        self, proc_batch_id: int, batch: List[StatusRow]
    ) -> Optional[List[StatusRow]]:
        logger.info("Entering function")
        logger.info(f"Trying to insert {len(batch)} documents")
        dane_docs = self._to_dane_docs(batch)
        r = requests.post(self.DANE_DOCS_ENDPOINT, data=json.dumps(dane_docs))
        if r.status_code == 200:
            # persist the response containing DANE.Document._id
            try:
                json_data = json.loads(r.text)
            except json.JSONDecodeError:
                logger.exception("Invalid JSON returned by DANE (register docs)")
                return None
            # if it cannot be persisted. Quit, because the program state will be corrupt
            if not self._persist_registered_batch(proc_batch_id, json_data):
                dane_batch_fn = self._get_batch_file_name(proc_batch_id)
                logger.critical(f"Could not persist DANE response to : {dane_batch_fn}")
                sys.exit()
            return self._to_updated_status_rows(batch, json_data)
        return None

    # sets the DANE.Document._id as proc_id for each status row and sets status to REGISTERED
    def _to_updated_status_rows(
        self, batch: List[StatusRow], dane_resp: dict
    ) -> List[StatusRow]:
        logger.info("Entering function")
        if dane_resp is None:
            logger.warning("DANE response was empty")
            return None

        # first extract all the DANE documents (failed or successful)
        logger.debug(json.dumps(dane_resp, indent=4, sort_keys=True))
        dane_docs = self.__extract_docs_by_state(dane_resp, DANEBatchState.SUCCESS)
        dane_docs.extend(self.__extract_docs_by_state(dane_resp, DANEBatchState.FAILED))

        # now map the target IDs (matching StatusRow.target_id) to each DANE Document for lookup
        dane_mapping = {doc.target["id"]: doc for doc in dane_docs}

        # update the StatusRows by setting the proc_id via the DANE Document._id
        for row in batch:
            row.proc_id = dane_mapping[row.target_id]._id
            row.status = ProcessingStatus.BATCH_REGISTERED
        return batch

    # returns a list of DANE Documents, of a certain state, from JSON data returned by the DANE API
    def __extract_docs_by_state(
        self, dane_api_resp: dict, state: DANEBatchState
    ) -> List[Document]:
        logger.info("Entering function")
        if dane_api_resp.get(state.value, None) is None:
            return []

        dane_docs = []
        for json_doc in dane_api_resp[state.value]:
            doc = self.__to_dane_doc(json_doc)
            if doc is not None:
                dane_docs.append(doc)
        return dane_docs

    # converts JSON data (part of DANE API response) into DANE Documents
    # TODO make sure to fix irregular JSON data in DANE core library
    def __to_dane_doc(self, json_data: dict) -> Optional[Document]:
        logger.info(f"Converting JSON to DANE Document {json_data}")
        if json_data is None:
            logger.warning("No json_data supplied")
            return None
        doc = json_data
        if json_data.get("document", None) is not None:
            doc = json_data["document"]
        return Document.from_json(doc) if doc and doc.get("_id") is not None else None

    def _persist_registered_batch(self, proc_batch_id: int, dane_resp: dict) -> bool:
        logger.info("Persisting DANE API response to disk")
        logger.info(dane_resp)
        try:
            with open(self._get_batch_file_name(proc_batch_id), "w") as f:
                f.write(json.dumps(dane_resp, indent=4, sort_keys=True))
                return True
        except Exception:
            logger.exception(f"Could not persist to {proc_batch_id}-batch.json")
            return False

    # called by DANEProcessingEnvironment.process_batch()
    def process_batch(self, proc_batch_id: int) -> Tuple[bool, int, str]:
        task_type = TaskType(self.DANE_TASK_ID)
        logger.info(f"going to submit {task_type.value} for the following doc IDs")
        doc_ids = self._get_doc_ids_of_batch(proc_batch_id)
        logger.info(doc_ids)
        if doc_ids is None:
            return (
                False,
                404,
                f"No doc_ids found in {self._get_batch_file_name(proc_batch_id)}",
            )
        task = {
            "document_id": doc_ids,
            "key": task_type.value,  # e.g. ASR, DOWNLOAD
        }
        logger.info(f"Submitting task to {self.DANE_TASK_ENDPOINT}")
        logger.debug(json.dumps(task))
        r = requests.post(self.DANE_TASK_ENDPOINT, data=json.dumps(task))
        return (
            r.status_code == 200,
            r.status_code,
            self.__parse_dane_process_response(r.text),
        )

    # TODO avoid persisting this JSON response in StatusRow.proc_status_msg
    def __parse_dane_process_response(self, dane_resp: str) -> str:
        logger.info("Parsing DANE response (TODO)")
        logger.info(dane_resp)

        # treat the errors as warnings, since some of them don't cause harm (see below)
        errors = self._extract_errors_from_dane_resp(dane_resp)
        for e in errors:
            logger.warning(e)

        return dane_resp

    """
    This function extracts the errors from DANE responses, so they can be logged
    NOTE: example responses returned by DANE
    {
        "success": [],
        "failed": [
            {
                "document_id": "7b1dcc4147fafb1cc089ca9d0ee46d382727cf1c",
                "error": "Task `BG_DOWNLOAD` already assigned to document `7b1dcc4147fafb1cc089ca9d0ee46d382727cf1c`"
            }
        ]
    }

    {
        "success": [],
        "failed": [
            {
                "document_id": "d035d6e713a151f3d94ce41a780068769f31bd11",
                "error": "[404] 'No document with id `d035d6e713a151f3d94ce41a780068769f31bd11` found'"
            }
        ]
    }
    """

    def _extract_errors_from_dane_resp(self, dane_resp: str) -> List[str]:
        logger.info("Extracting error messages from DANE response")
        errors = []
        try:
            data = json.loads(dane_resp)
            for msg in data.get("failed", []):
                if "error" in msg:
                    errors.append(msg["error"])
        except json.JSONDecodeError as e:
            logger.exception(e)
        return errors

    # returns a list of DANE Tasks when done
    def monitor_batch(self, proc_batch_id: int, verbose=False) -> List[Task]:
        logger.info(f"\t\tMonitoring DANE batch: {proc_batch_id}")
        start_time = perf_counter()
        tasks_of_batch = []
        while True:  # infinite loop, until there are no more running tasks
            tasks_of_batch = self.get_tasks_of_batch(proc_batch_id, [])
            task_type = TaskType(self.DANE_TASK_ID)
            logger.info(f"Found {len(tasks_of_batch)} tasks")
            logger.info("*" * 50)

            # log the raw JSON status of ALL tasks (verbose only)
            status_overview = self._generate_tasks_overview(tasks_of_batch)
            if verbose:
                self._log_all_tasks_verbose(status_overview)

            # log a status overview per (type of) dane_task (e.g. ASR, DOWNLOAD, etc)
            logger.info(f"Reporting on the {task_type.value} task")
            self._log_status_of_dane_task_type(status_overview, task_type)

            # TODO report and work on the dictionary with statusses to return
            logger.info(f"Waiting for {self.MONITOR_INTERVAL} seconds")
            sleep(self.MONITOR_INTERVAL)
            logger.info("-" * 50)
            if not self._contains_running_tasks(tasks_of_batch):
                logger.info(
                    f"All tasks were done, monitoring proc_batch {proc_batch_id} stopped"
                )
                break

            # now also check if we are not waiting for failed dependencies
            if not self._check_unfinished_dependencies_ok(
                tasks_of_batch, self.DANE_TASK_ID
            ):
                logger.warning(
                    f"The remaining tasks all have failed dependencies; monitoring proc_batch {proc_batch_id} stopped"
                )
                break

            logger.info("Not done, continuing to monitor...")

        logger.info(
            f"Time it took to finish this batch {(perf_counter() - start_time)} seconds"
        )
        logger.debug(tasks_of_batch)
        return tasks_of_batch

    # Check if all tasks with proc_batch_id are done running
    def is_proc_batch_done(self, proc_batch_id: int) -> bool:
        logger.info("Entering function")
        return (
            self._contains_running_tasks(self.get_tasks_of_batch(proc_batch_id, []))
            is False
        )  # done if there are no running tasks remaining

    # Check if all supplied tasks have (un)successfully run
    # A task is still running if it is: QUEUED, CREATED or has UNFINISHED_DEPENDENCY
    def _contains_running_tasks(self, tasks_of_batch: List[Task]) -> bool:
        logger.info(
            f"Checking {len(tasks_of_batch)} tasks to see if they are any running"
        )
        if tasks_of_batch is None:
            logger.warning("Called with tasks_of_batch is None")
            return False
        return (
            len(
                list(
                    filter(
                        lambda x: x.state
                        in [
                            TaskState.QUEUED.value,
                            TaskState.UNFINISHED_DEPENDENCY.value,
                            TaskState.CREATED.value,
                        ],
                        tasks_of_batch,
                    )
                )
            )
            != 0
        )

    # checks if there are failed dependencies for self.DANE_TASK_ID
    def _check_unfinished_dependencies_ok(
        self, tasks_of_batch: List[Task], leaf_task: str
    ) -> bool:
        logger.info(
            f"Checking {len(tasks_of_batch)} tasks of batch for failed dependencies"
        )
        # check the unfinished dependencies
        tasks_with_unfinished_deps = list(
            filter(
                lambda x: x.state == TaskState.UNFINISHED_DEPENDENCY.value,
                tasks_of_batch,
            )
        )
        # it's ok if there are no tasks with unfinished deps (it's handled in the monitor function)
        if not tasks_with_unfinished_deps:
            logger.info("There are no tasks with unfinished dependencies")
            return True

        logger.info(
            f"Checking {len(tasks_with_unfinished_deps)} tasks with unfinished dependencies"
        )
        failed_count = 0
        for t in tasks_with_unfinished_deps:
            dependant_tasks = self._get_tasks_of_document(t.doc_id, leaf_task)
            failed_count += 1 if self._contains_failed_deps(dependant_tasks) else 0

        # as long as not all dependencies have failed, it's ok
        return len(tasks_with_unfinished_deps) > failed_count

    # check if any dependant tasks failed
    def _contains_failed_deps(self, tasks: List[Task]) -> bool:
        logger.info(f"Checking {len(tasks)} tasks (dependencies) to see if they failed")
        return any(
            t.state
            in [
                TaskState.BAD_REQUEST.value,
                TaskState.ACCESS_DENIED.value,
                TaskState.NOT_FOUND.value,
                TaskState.ERROR.value,
                TaskState.ERROR_INVALID_INPUT.value,
                TaskState.ERROR_PROXY.value,
            ]
            for t in tasks
        )

    # NOTE: (not used, but maybe useful later) check if there are any pending dependant tasks
    def _contains_pending_deps(self, tasks: List[Task]) -> bool:
        return any(
            t.state
            in [
                TaskState.QUEUED.value,
                TaskState.UNFINISHED_DEPENDENCY.value,
                TaskState.CREATED.value,
            ]
            for t in tasks
        )

    # NOTE: (not used, but maybe useful later) check if ALL dependant tasks were done
    def _dependencies_met(self, tasks: List[Task]) -> bool:
        return all(
            t.state
            in [
                TaskState.SUCCESS.value,
            ]
            for t in tasks
        )

    # returns an overview in the form:
    # {
    #   "ASR" : {
    #       102 : {
    #           "msg" : "Status message",
    #           "tasks" : ["id1", "id2"]
    #       }
    #   }
    # }
    def _generate_tasks_overview(self, tasks_of_batch: List[Task]) -> dict:
        logger.info("Entering function")
        status_overview: dict = {}
        for t in tasks_of_batch:
            task_state = f"{t.state}"
            if t.key in status_overview:
                if task_state in status_overview[t.key]["states"]:
                    status_overview[t.key]["states"][task_state]["tasks"].append(t.id)
                else:
                    status_overview[t.key]["states"][task_state] = {
                        "msg": t.message,
                        "tasks": [t.id],
                    }
            else:
                status_overview[t.key] = {
                    "states": {task_state: {"msg": t.message, "tasks": [t.id]}}
                }
        return status_overview

    def _log_all_tasks_verbose(self, status_overview: dict):
        logger.info("Entering function")
        logger.debug(json.dumps(status_overview, indent=4, sort_keys=True))

    def _log_status_of_dane_task_type(self, status_overview, dane_task: TaskType):
        logger.info(
            f"Showing all processing states for current DANE batch for all tasks of type: {dane_task.value}"
        )
        states = status_overview.get(dane_task.value, {}).get("states", {})
        c_unknown = 0
        for state in states.keys():
            state_count = len(states[state].get("tasks", []))
            try:
                ts = TaskState(int(state))
                logger.info(f"Number of {ts.name} tasks: {state_count}")
            except ValueError:
                logger.info(f"Found an unmapped DANE status code: {state}")
                c_unknown += state_count

        logger.info(f"Number of UNKNOWN tasks: {c_unknown}")

    # NOTE: DANE will create a new document if the target_id + creator_id does not exist,
    # meaning it's important to assign a unique DANE_BATCH_PREFIX for each DANE env/server (API)
    def _to_dane_docs(self, status_rows: List[StatusRow]) -> Optional[List[dict]]:
        logger.info("Entering function")
        if not status_rows or len(status_rows) == 0:
            logger.warning("No data provided")
            return None

        if status_rows[0].proc_batch_id is None:
            logger.warning("The provided status_rows MUST contain a proc_batch_id")
            return None

        return [
            Document(
                {
                    "id": sr.target_id,
                    "url": sr.target_url,
                    "type": "Video",
                },
                {
                    "id": self._get_proc_batch_name(sr.proc_batch_id),
                    "type": "Organization",
                },
            ).to_json()
            for sr in status_rows
        ]

    def _get_proc_batch_name(self, proc_batch_id):
        return f"{self.BATCH_PREFIX}_{proc_batch_id}"
