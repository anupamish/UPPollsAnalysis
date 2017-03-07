# Makefile for UP Polls Analysis
# Anupam Mishra
# Gautam Budhha University, 2017

# Run `make init` to install the python requirements and project requisites
init:
	mkdir -p analysis
	pip install -r requirements.txt

# Run `make stream` to stream the tweets for 1 hour
stream:
	python stream.py

# Run `make analyse` to analyse all the tweets in the database
analyse:
	python analyse.py

.PHONY: init start analyse
