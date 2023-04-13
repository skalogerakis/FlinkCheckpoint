#!/bin/bash

PATH_FILE=$1

if [ -z "$1" ]; then
    echo "No argument supplied"
else
  echo "Execute application for ${PATH_FILE}"
  sh ./media/localdisk/skalogerakis/flink/bin/flink run /media/localdisk/skalogerakis/recovery/FlinkCheckpoint/target/FlinkCheckpoint-1.0-SNAPSHOT.jar -c hdfs:///flink-checkpoints -file /media/localdisk/skalogerakis/recovery/{PATH_FILE}
fi

