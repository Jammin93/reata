#!/bin/bash

INIT_WD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

cd $INIT_WD/docsrc

make clean
make github

sphinx-build . ./_build
