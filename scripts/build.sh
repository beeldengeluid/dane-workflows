#!/bin/sh

# NB: run this script inside a virtual env via `poetry shell``

# navigate to root
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$SCRIPTPATH"
cd ../

poetry install
poetry build
