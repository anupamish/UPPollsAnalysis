
init:
	pip install -r requirements.txt

start:
	python stream.py

analyse:
	python analyse.py

.PHONY: init start analyse
