#!/bin/bash

export className=SparkJob

nohup spark-submit \
  --class com.liang.spark.job.${className} \
  --name ${className} \
  --proxy-user liang \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g --driver-cores 1 \
  --conf spark.driver.memoryOverhead=512m \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
  --conf spark.dynamicAllocation.executorIdleTimeout=300s \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=32 \
  --executor-memory 9g --executor-cores 8 \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=512m \
  --queue offline \
  --conf spark.memory.fraction=0.5 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.yarn.am.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.yarn.cluster.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --files log4j-all.properties,config.yml \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.priority=999 \
  --conf spark.sql.shuffle.partitions=1024 \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \
  ./spark-1.0.jar config.yml >log 2>&1 &
