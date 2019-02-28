#!/bin/bash

terraform output --json | python3 tf2inventory.py > aws.yml
