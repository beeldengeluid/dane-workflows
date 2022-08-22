# test a full workflow
if __name__ == "__main__":
    from dane_workflows.util.base_util import load_config
    from dane_workflows.task_scheduler import TaskScheduler
    from dane_workflows.status import SQLiteStatusHandler
    from dane_workflows.data_provider import ExampleDataProvider
    from dane_workflows.data_processing import ExampleDataProcessingEnvironment
    from dane_workflows.exporter import ExampleExporter

    print("Reading local config...")
    config = load_config("config-example.yml")
    print(config)

    print("Starting example workflow...")

    ts = TaskScheduler(
        config,
        SQLiteStatusHandler,
        ExampleDataProvider,
        ExampleDataProcessingEnvironment,
        ExampleExporter,
    )

    ts.run()

    print("All done")
