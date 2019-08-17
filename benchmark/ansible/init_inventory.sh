#!/bin/bash

terraform output --json | python3 "${BASH_SOURCE%/*}"/tf2inventory.py > remote.yml
