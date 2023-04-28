#!/bin/bash

# For Symbol Event Counter
#PATH_FILE=$1
#FULL_PATH="/media/localdisk/skalogerakis/recovery/${PATH_FILE}"
#
#if [ -z "$1" ]; then
#    echo "No argument supplied"
#else
#  echo "Execute application for ${PATH_FILE}"
#  /media/localdisk/skalogerakis/flink/bin/flink run -d /media/localdisk/skalogerakis/recovery/FlinkCheckpoint/target/FlinkCheckpoint-1.0-SNAPSHOT.jar -c hdfs:///flink-checkpoints -file ${FULL_PATH}
#fi


SIZE=$1
#FULL_PATH="/media/localdisk/skalogerakis/recovery/${PATH_FILE}"

if [ -z "$1" ]; then
    echo "No argument supplied"
else
  sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches "
  sleep 1

  echo "Execute Synthetic Source Generator for size ${SIZE} MB"
  /media/localdisk/skalogerakis/flink/bin/flink run -d /media/localdisk/skalogerakis/recovery/FlinkCheckpoint/target/FlinkCheckpoint-1.0-SNAPSHOT.jar -c hdfs:///flink-checkpoints -size ${SIZE}
fi