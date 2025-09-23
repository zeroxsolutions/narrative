#!/bin/bash
# Run tests for all Go modules in the repository

source "$(dirname "$0")"/utils.sh

if ! commandExist go;
then
  echo 'please install golang'
  exit 1
fi

project_dir="$(cd -- "$(dirname -- "$0")/.." &>/dev/null && pwd -P)"

cd $project_dir

# Find all directories containing a go.mod file
modules=$(find . -name "go.mod" -exec dirname {} \;)

# Loop through each module and run tests
for module in $modules; do
    echo "Running tests in $module..."
    (cd $module && go test ./...)
done