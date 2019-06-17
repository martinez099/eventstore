#!/usr/bin/env bash

python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. event_store.proto
