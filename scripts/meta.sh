#!/bin/bash
echo "Creating necessary directories..."

set -o allexport; source .env; set +o allexport

if [[ ! -d $AG_GROUPER_HOME ]]; then
  sudo mkdir -p $AG_GROUPER_HOME
  sudo chown $USER $AG_GROUPER_HOME
fi

if [[ -d $AG_GROUPER_HOME/processed ]]; then
   echo "Already present, skipping..."
else
    mkdir -p $AG_GROUPER_HOME/processed
fi

if [[ -d $AG_GROUPER_HOME/processed/fetcher ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/processed/fetcher
fi

if [[ -d $AG_GROUPER_HOME/processed/grouper ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/processed/grouper
fi

if [[ -d $AG_GROUPER_HOME/processed/combined ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/processed/combined
fi

if [[ -d $AG_GROUPER_HOME/processed/hub ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/processed/hub
fi

if [[ -d $AG_GROUPER_HOME/uploads/django ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/uploads/django
fi

if [[ ! -d /var/run/celery ]]; then
  sudo mkdir -p /var/run/celery
  sudo chown $USER /var/run/celery
fi

if [[ ! -d /var/log/celery ]]; then
  sudo mkdir -p /var/log/celery
  sudo chown $USER /var/log/celery
fi

if [[ -d $AG_GROUPER_HOME/processed/fetcher/snapshots ]]; then
   echo "Already present, skipping..."
else
  mkdir -p $AG_GROUPER_HOME/processed/fetcher/snapshots
fi