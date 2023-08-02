#!/bin/bash



SIZE=$1
#FULL_PATH="/media/localdisk/skalogerakis/recovery/${PATH_FILE}"

if [ -z "$1" ]; then
    echo "No argument supplied"
else
  sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches "
  sleep 1

  echo "Execute Synthetic Source Generator for size ${SIZE} MB"
  /home/skalogerakis/flink/bin/flink run -d /home/skalogerakis/Documents/Workspace/FlinkCheckpoint/target/FlinkCheckpoint-1.0-SNAPSHOT.jar -c hdfs:///flink-checkpoints -size ${SIZE}
fi