#!/bin/bash
docker rm -f parsersql-test-mysql parsersql-test-pgsql 2>/dev/null || true
echo "Test backends stopped."
