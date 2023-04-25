#!/bin/bash

JOB_ID=$1

if [ -z "$1" ]; then
  echo "No argument supplied. Please provide Job Id"
else
  echo "RestartTime for Job: ${JOB_ID}"
#  curl http://139.91.183.36:8081/jobs/${JOB_ID}/metrics?get=restartingTime,lastCheckpointRestoreTimestamp
  curl http://139.91.183.36:8081/jobs/${JOB_ID}
  echo ""
fi