#!/bin/bash

# docker image name
IMAGE="datadiverdev/postgres-jupyter"

# docker application hostname
HOST="localhost"

# docker application name
NAME="datadiver-postgres"

# docker application ports
PORT_DB="5432:5432"
PORT_JUPYTER="80:80"
PORT_DASHBOARD="8080:8080"

# run the docker image
cmd=(docker run --name "$NAME" --hostname "$HOST" -p "$PORT_DB" -p "$PORT_JUPYTER" -p "$PORT_DASHBOARD" -d "$IMAGE")
"${cmd[@]}"
