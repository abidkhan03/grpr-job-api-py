#!/usr/bin/env bash

echo "Removing old containers..."
docker-compose down --remove-orphans

echo "Building new containers..."
docker-compose up -d --build --force-recreate
