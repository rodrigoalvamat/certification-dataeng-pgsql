#!/bin/bash

# get the full path of the script and its directory
SCRIPT_PATH="${BASH_SOURCE}"
while [ -L "${SCRIPT_PATH}" ]; do
  SCRIPT_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"
  SCRIPT_PATH="$(readlink "${SCRIPT_PATH}")"
  [[ ${SCRIPT_PATH} != /* ]] && SCRIPT_PATH="${SCRIPT_DIR}/${SCRIPT_PATH}"
done
SCRIPT_PATH="$(readlink -f "${SCRIPT_PATH}")"
SCRIPT_DIR="$(cd -P "$(dirname -- "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"

# docker image name
IMAGE="datadiverdev/postgres-conda-jupyter"

# docker build paths
BUILD_DIR=""${SCRIPT_DIR}"/.."
DOCKERFILE=""${SCRIPT_DIR}"/Dockerfile"

# build the docker image
cmd=(docker build "$BUILD_DIR" -f "$DOCKERFILE" -t "$IMAGE")
"${cmd[@]}"
