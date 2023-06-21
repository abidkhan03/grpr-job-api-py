#!/usr/bin/env bash

CELERY_LOG_PATH=/var/log/celery

if [[ ! -d $CELERY_LOG_PATH ]]; then
  echo "Celery log path doesn't exist..."
  exit
fi

LATEST_LOG_FILE=$(ls -ltr $(find $CELERY_LOG_PATH -type f) | tail -1 | sed 's/  */:/g' | cut -d : -f 10)

echo "Checking logs from latest celery log file $LATEST_LOG_FILE..."
tail -f $LATEST_LOG_FILE
