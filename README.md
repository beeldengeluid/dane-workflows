# Introduction

Python library for creating "processing workflows" that use [DANE environments](https://github.com/beeldengeluid/dane-environments), which in a nutshell offer, depending on the setup of each environment, an API for some kind of multi-media processing, e.g.:

* Automatic Speech Recognition
* Named Entity Extraction
* Computer Vision algorithms
* Any kind of Machine Learning algorithm

This Python library is however not limited to using [DANE](https://github.com/CLARIAH/DANE), but cannot also be used to hook up any API that does something with generating certain data from certain input data.

# Achitecture

The following image illustrates the dane-workflows architecture:

![Image](architecture.png)

The following section details more about concepts illustrated in the image.

# Definition of a workflow

A workflow is able to iteratively:
- obtain input/source data from a `DataProvider`
- send it to a `ProcessingEnvironment` (e.g. DANE environment)
- wait for the processing environment to complete its work
- obtain results from the processing environment
- pass results to an `Exporter`, which typically reconsiles the processed data with the source data     

As mentioned in the definition of a workflow, this Python library works with the following components/concepts:

## TaskScheduler

Main process that handles all the steps described in the [Definition of a workflow]()

## StatusHandler

Keeps track of the workflow status, esuring recovery after crashes. By default the status is persisted to a SQLite database file, using the `SQLiteStatusHandler` but other implementations can be made by subclassing `StatusHandler`. 

## StatusMonitor

**Note**: This component is currently implemented and not yet available. 

Runs on top of the StatusHandler database and visualises the overall progress of a workflow in a human-readable manner (e.g. show the % of successfully/failed processed items)

## DataProvider

Iteratively called by the `TaskScheduler` to obtain a new batch of source data. No default implementations are available (yet), since there are many possible ways one would want to supply data to a system. Simply subclass from `DataProvider` to have full control over your input flow.

## DataProcessingEnvironment

Iteratively called by the `TaskScheduler` to submit batches of data to an (external) processing environment. Also takes care of obtaining the output of finished processes from such an environment.

This library contains a full implementation, `DANEEnvironment`, for interacting with [DANE environments](https://github.com/beeldengeluid/dane-environments), but other environments/APIs can be supported by subclassing from `ProcessingEnvironment`.

## Exporter

Called by the `TaskScheduler` with output data from a processing environment. No default implementation is available (yet), since this is typically the most use-case sensitive part of any workflow, meaning you should decide what to do with the output data (by subclassing `Exporter`).

# Getting started

## Prerequisites

* Python >= 3.8 <= 3.10
* [Poetry](https://python-poetry.org/)

## Installation

Install via pypi.org, using e.g.

```
pip install dane-workflows
```

### local development

Run `poetry install`. After completion run:

```
poetry shell
```

To test the contents of this repository works well, run:

```
./scripts/check-project.sh
```

TODO finalise

# Usage

After installing dane-workflows in your local environment, you can run an example workflow with:

```
python main.py
```

This example script uses `config-example.yml` to configure and run a workflow using the following implementations:

- **DataProvider**: ExampleDataProvider (with two dummy input documents)
- **DataProcessingEnvironment**: ExampleDataProcessingEnvironment (mocks processing environment)
- **StatusHandler**: SQLiteStatusHandler (writes output to `./proc_stats/all_stats.db`)
- **Exporter**: ExampleExporter (does nothing with results)

To setup a workflow for your own purposes, consider the following:

## What data do I want to process?

We've provided the `ExampleDataProvider` to easily feed a workflow with a couple of files (via config.`yml`). This is mostly for testing out your workflow.

Mostly likely you'll need to implement your own `DataProvider` by subclassing it. This way you can e.g. load your input data from a database, spreadsheet or whatever else you need.

## Which processing environment will I use?

Since this project is developed to at least interface with running [DANE environments](https://github.com/beeldengeluid/dane-environments) we've provided `DANEEnvironment` as a default implementation of `DataProcessingEnvironment`.

In case you'd like to call any other tool for processing your data, you're required to implement a subclass of `DataProcessingEnvironment`.

## What I will I do with the output of the processing environment?

After your `DataProcessingEnvironment` has processed a batch of items from your `DataProvider` the `TaskScheduler` hands over the output data to your subclass of `Exporter`. 

Since this is the most use-case dependant part of any workflow, we do not provide any useful default implementation. 

Note: `ExampleExporter` is only used as a placeholder for tests or dry runs.

# Roadmap

- [x] Implement more advanced recovery
- [x] Add example workflows (refer in README)
- [x] Finalise initial README
- [ ] Add [Python docstring](https://www.askpython.com/python/python-docstring)

See the [open issues](https://github.com/beeldengeluid/dane-workflows/issues) for a full list of proposed features, known issues and user questions.


# License
Distributed under the MIT License. See `LICENSE.txt` for more information.


# Contact
Use the [issue tracker](https://github.com/beeldengeluid/dane-workflows/issues) for any questions concerning this repository

Project Link: https://github.com/beeldengeluid/dane-workflows

Codemeta.json requirements: https://github.com/CLARIAH/clariah-plus/blob/main/requirements/software-metadata-requirements.md