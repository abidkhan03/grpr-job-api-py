#!/bin/bash
./scripts/meta.sh
echo "Stopping already running service..."
pkill -f celery
set -o allexport; source .env; set +o allexport
pipenv install
cd server
pipenv run python manage.py migrate
echo "Deploying server..."
pipenv run celery multi stop -A utils
pipenv run celery multi start 2 -A utils -l INFO --pidfile=/var/run/celery/%n.pid --logfile=/var/log/celery/%n%I.log
sudo systemctl restart gunicorn.service
echo "Server deployed successfully!"
