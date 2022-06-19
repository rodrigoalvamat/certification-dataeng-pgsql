#!/bin/bash

# set streamlit environment 
export PATH="$PATH:$HOME/.local/bin"

# start streamlit server
streamlit run --server.port=8080 "$HOME/workspace/src/app.py"
