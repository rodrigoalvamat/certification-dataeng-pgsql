#!/bin/bash

# set jupyter environment 
export PATH="$PATH:$HOME/.local/bin"

# start jupyter server
jupyter-lab --ip=* --port=80 --notebook-dir=/home/"$USER"/workspace --no-browser
