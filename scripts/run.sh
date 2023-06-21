#!/bin/bash
./scripts/meta.sh
pkill -f celery
set -o allexport; source .env; set +o allexport
pipenv install
cd server
pipenv run python manage.py migrate
pipenv run celery multi stop -A utils
pipenv run celery multi start 2 -A utils -l INFO --pidfile=/var/run/celery/%n.pid --logfile=/var/log/celery/%n%I.log
cd ..
pipenv run python server/manage.py runserver 0.0.0.0:8000
