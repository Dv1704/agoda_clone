#!/bin/bash
set -ex

echo "Building Linux binary for deployment..."
GOOS=linux GOARCH=amd64 go build -o agoda-data-pipeline main.go

IP="144.91.111.151"
PASS="P4K9s8bvtTv6xu77"
USER="root"
DIR="/root/agoda_data_pipeline"

export SSHPASS=$PASS

echo "Setting up remote directory..."
sshpass -e ssh -o StrictHostKeyChecking=no $USER@$IP "mkdir -p $DIR"

echo "Transferring files..."
sshpass -e rsync -avz --exclude '.git' --exclude 'data/stable/output' --exclude 'data/test/output' ./ $USER@$IP:$DIR/

echo "Starting services on remote server..."
sshpass -e ssh -o StrictHostKeyChecking=no $USER@$IP "cd $DIR && \
  chmod +x ./agoda-data-pipeline && \
  docker-compose down || true && \
  docker-compose up -d && \
  pkill -f './agoda-data-pipeline' || true && \
  nohup ./agoda-data-pipeline > app.log 2>&1 &"

echo "Deploy finished! Connect to http://$IP:8080"
