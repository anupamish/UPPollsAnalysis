# UP Polls Analysis

## Setup
- Install the `make` package.
- Run `make init` to install all the python requirements and directories.
- If `make init` fails and complains `Permission Denied`, run it with `sudo`.

## Run
Before you run the project, make sure that
- the MySQL server is running, and
- `config.py` file exists with necessary tokens and configs.

Now you should be ready to run the project.
- Issue `make start` to start the streaming script followed by the analysis script.
- You can also call `python stream.py` and `python analyse.py` in order to do the required task.
