#!/bin/bash

# Create a virtual environment
virtualenv --python=python3 --clear venv

# Activate the virtual environment
source venv/bin/activate

# Install dependencies from requirements.txt
pip install -r requirements.txt

