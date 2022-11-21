from dane_workflows.status import ExampleStatusHandler
from dane_workflows.util.base_util import import_dane_workflow_class


def test_get_pretty_config(example_exporter_config):
    status_handler = ExampleStatusHandler(example_exporter_config)
    exporter_class = import_dane_workflow_class(
        example_exporter_config["EXPORTER"]["TYPE"]
    )
    exporter = exporter_class(example_exporter_config, status_handler)
    assert exporter.get_pretty_config() == {}
