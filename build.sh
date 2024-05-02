#!/bin/bash

docker buildx build -t mainnetpat/graph-node -f docker/Dockerfile --platform linux/amd64 --target graph-node --push .
