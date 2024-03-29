import logging


logger = logging.getLogger(__name__)


"""
TODO integrate this into these PR's (for DANE and DANE-server):
- https://github.com/CLARIAH/DANE/pull/16
- https://github.com/CLARIAH/DANE-server/pull/5

After these PRs are merged: get rid of this module and adapt the dane_util.py
to call the new DANE API functions instead
"""


# query for fetching the result of a certain task
def result_of_task_query(task_id: str):
    logger.debug("Generating result_of_task_query")
    return {"query": {"parent_id": {"type": "result", "id": task_id}}}


# query for fetching the task of the document with a certain target.id and DANE Task.key
def task_of_target_id_query(target_id: str, dane_task_id: str, base_query: bool = True):
    task_query = {
        "bool": {
            "must": [
                {
                    "has_parent": {
                        "parent_type": "document",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "query_string": {
                                            "default_field": "target.id",
                                            "query": target_id,
                                        }
                                    }
                                ]
                            }
                        },
                    }
                },
                {"query_string": {"default_field": "task.key", "query": dane_task_id}},
            ]
        }
    }
    if base_query:
        query: dict = {}
        query["_source"] = ["task", "created_at", "updated_at", "role"]
        query["query"] = task_query
        return query
    return task_query


# query for fetching the result of the document with a certain target.id
def result_of_target_id_query(target_id: str, dane_task_id: str):
    logger.debug("Generating result_of_target_id_query")
    sub_query = task_of_target_id_query(target_id, dane_task_id, False)
    return {
        "query": {
            "bool": {
                "must": [
                    {"has_parent": {"parent_type": "task", "query": sub_query}},
                    {"exists": {"field": "result.payload"}},
                ]
            }
        }
    }


# query for fetching all tasks for documents with a certain creator.id (used to record batches)
def tasks_of_batch_query(
    proc_batch_name: str, offset: int, size: int, dane_task_id: str, base_query=True
) -> dict:
    logger.debug("Generating tasks_of_batch_query")
    match_creator_query = {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "default_field": "creator.id",
                        "query": '"{}"'.format(proc_batch_name),
                    }
                }
            ]
        }
    }
    tasks_query = {
        "bool": {
            "must": [
                {
                    "has_parent": {
                        "parent_type": "document",
                        "query": match_creator_query,
                    }
                },
                {
                    "query_string": {
                        "default_field": "task.key",
                        "query": dane_task_id,
                    }
                },
            ]
        }
    }
    if base_query:
        query: dict = {}
        query["_source"] = ["task", "created_at", "updated_at", "role"]
        query["from"] = offset
        query["size"] = size
        query["query"] = tasks_query
        return query
    return tasks_query


# query for fetching all results for documents with a certain creator.id (used to record batches)
# FIXME: in case the underlying tasks mentioned: "task already assigned", the results will
# NOT be found this way
def results_of_batch_query(
    proc_batch_name: str, offset: int, size: int, dane_task_id: str
):
    logger.debug("Generating results_of_batch_query")
    sub_query = tasks_of_batch_query(proc_batch_name, offset, size, dane_task_id, False)
    return {
        "_source": ["result", "created_at", "updated_at", "role"],
        "from": offset,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "has_parent": {
                            "parent_type": "task",
                            "query": sub_query,
                        }
                    },
                    {
                        "exists": {"field": "result.payload"}
                    },  # only results with a payload
                ]
            }
        },
    }
