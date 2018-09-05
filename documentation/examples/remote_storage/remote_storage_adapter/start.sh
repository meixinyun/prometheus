#!/bin/bash
export INFLUXDB_PW=admin
nohup ./remote_storage_adapter -influxdb-url http://127.0.0.1:8086 log.level debug  -influxdb.database prometheus -influxdb.username admin 2>&1 1 >> adapter.log &
