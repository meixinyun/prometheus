#!/bin/bash
nohup ./prometheus --web.enable-lifecycle --config.file="prometheus.yml" --web.enable-admin-api  --log.level=debug 2>&1 &
