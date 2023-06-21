# Keyword Grouper API

A django based REST API to run fetcher, grouper and combined jobs

## Pre-requisites

- Python
- Pipenv (follow [this link](https://pypi.org/project/pipenv/) to install)

## Dev Setup

### Local Environment

Follow `.env.sample` to create `.env`

Setup Pipenv environment by running

```bash
pipenv install
```

Some requirements have to be installed manually inside pipenv shell

```bash
pipenv shell
pip install sentence-transformers # inside pipenv shell
```

```bash
./scripts/run.sh
```

### Dockerized Environment

Create a folder `credentials` and put your GCS credentials file inside.

Follow `.env.docker.sample` to create `.env.docker` and run

```bash
./scripts/docker-build.sh
```

> You can see the logs for your dockerized environment using this utility
> script
>
> ```
> ./scripts/docker-logs.sh
> ```

## Deployment

```bash
./scripts/deploy.sh
```

This will run the server at `http://localhost:8000`

To stop the service

```bash
./scripts/stop-service.sh
```

## Project Structure

```
+-- server/             # server code
+-- scripts/            # bash scripts
+-- .env.sample
+-- .gitignore
+-- README.md           # project documentation
```
