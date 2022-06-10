#!/bin/bash

# load conda environment
set -euo pipefail
source /opt/conda/etc/profile.d/conda.sh

# activate conda environment
set +euo pipefail
conda activate py310

# start jupyter lab server
set -euo pipefail
jupyter-lab --ip=* --port=80 --notebook-dir=/home/"$USER"/workspace --no-browser
