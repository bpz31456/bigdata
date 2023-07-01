#!/bin/bash
flink_home=/opt/install/flink-1.15.2
${flink_home}/bin/flink run-application -t yarn-application \
-Djobmanager.memory.process.size=1024m \
-Dtaskmanager.memory.process.size=1024m \
-Dtaskmanager.numberOfTaskSlots=1 \
${flink_home}/job_jars/flink_task2-1.0-SNAPSHOT.jar