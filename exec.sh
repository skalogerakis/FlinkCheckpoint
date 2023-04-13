#!/bin/bash

PATH_FILE=$1
FULL_PATH="/media/localdisk/skalogerakis/recovery/${PATH_FILE}"

if [ -z "$1" ]; then
    echo "No argument supplied"
else
  echo "Execute application for ${PATH_FILE}"
  /media/localdisk/skalogerakis/flink/bin/flink run -d /media/localdisk/skalogerakis/recovery/FlinkCheckpoint/target/FlinkCheckpoint-1.0-SNAPSHOT.jar -c hdfs:///flink-checkpoints -file ${FULL_PATH}
fi

