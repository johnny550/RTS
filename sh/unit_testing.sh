#!/bin/bash

echo "Running tests..."
go test -v rts-aws/pkg/tests -count=1 -bench -cover -coverpkg=./... -coverprofile out

