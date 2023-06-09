create-venv:
	python3 -m venv venv

activate-venv:
	. venv/bin/activate

install-requirements:
	pip install -r requirements.txt

setup: create-venv activate-venv install-requirements
