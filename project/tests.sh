
#!/bin/bash

# python ../project/tests.py

# Exit immediately if a command exits with a non-zero status
set -e

echo "Running tests..."

# Run the tests
pytest project/tests.py
