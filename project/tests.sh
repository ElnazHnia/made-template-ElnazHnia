
#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Running tests..."

# Assuming tests.sh is called from the root directory and tests.py is inside 'project'
pytest project/tests.py
