#!/
#!/bin/bash

# Define absolute paths
PROJECT_DIR="/Users/pgarg/Desktop/mcp-kinetica"
VENV_DIR="$PROJECT_DIR/mcp-env-py311"

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# (Optional but recommended) Set PYTHONPATH
export PYTHONPATH="$PROJECT_DIR"

# Run the server script directly
python "$PROJECT_DIR/src/server.py"

