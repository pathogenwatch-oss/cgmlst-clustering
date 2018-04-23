#!/bin/bash
set -eu

LEVEL=${1:-"minor"}

echo Creating new $LEVEL release...

npm version $LEVEL
git push && git push --tags
