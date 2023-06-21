#!/bin/bash
cd /usr/src/app/server
celery multi start 2 -A utils -l INFO --pidfile=/var/run/celery/%n.pid --logfile=/var/log/celery/%n%I.log
cd /usr/src/app
python server/manage.py runserver 0.0.0.0:8000
