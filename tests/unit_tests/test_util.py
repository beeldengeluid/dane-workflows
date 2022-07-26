from dane_workflows.status import ProcessingStatus, StatusRow, ErrorCode

class LoggerMock(object):
    def __enter__(self):
        pass

    def __exit__(self, one, two, three):
        pass

    def info(self, info_string):
        pass

    def error(self, info_string):
        pass

def new_batch(
    source_batch_id: int, status: ProcessingStatus, proc_error_code: ErrorCode = None, size: int=100
):
    offset = source_batch_id * size
    return [
        StatusRow(
            target_id=str(
                x
            ),  # Use this to reconsile results with source catalog (DANE.Document.target.id)
            target_url=f"http://{x}",  # So DataProcessingEnvironment can get to the content (DANE.Document.target.url)
            status=status,  # a ProcessingStatus value
            source_batch_id=0,  # source_batch_id (automatically incremented)
            source_batch_name="batch_0",  # also store "semantic" batch ID
            source_extra_info="unit_test",  # allow data providers to store a bit of extra info
            proc_batch_id=None,  # provided by the TaskScheduler, increments
            proc_id=None,  # ID assigned by the DataProcessingEnvironment
            proc_status_msg=None,  # Human readable status message from DataProcessingEnvironment
            proc_error_code=proc_error_code,  # an ErrorCode value
        )
        for x in range(offset, offset + size)
    ]
