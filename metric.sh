#!/bin/bash

JOB_ID=$1

if [ -z "$1" ]; then
  echo "No argument supplied"
else
  echo "RestartTime for Job: ${JOB_ID}"
  curl http://139.91.183.36:8081/jobs/${JOB_ID}/metrics?get=restartingTime
  echo ""
fi