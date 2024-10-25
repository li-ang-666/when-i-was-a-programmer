#!/bin/bash

nohup java -server \
  -XX:+UseParallelGC -XX:+UseParallelOldGC \
  -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
  -Dlog4j.configuration=file:./log4j.properties \
  -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 \
  -Xms3g -Xmx3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m \
  -cp ./repair-1.0-jar-with-dependencies.jar com.liang.repair.launch.Launcher \
  --impl $1 \
  --arg1 $2 \
  --arg2 $3 \
  --arg3 $4 \
  --arg4 $5 \
  --arg5 $6 \
  --arg6 $7 >log 2>&1 &

[root@emr-header-1 kafka]# jps | grep 4583
4583 KafkaConsumeJob
[root@emr-header-1 kafka]# jps -l | grep 4583
4583 com.liang.common.launch.KafkaConsumeJob
[root@emr-header-1 kafka]# jps -m | grep 4583
4583 KafkaConsumeJob --bootStrapServers 172.17.89.57:9092,172.17.89.58:9092,172.17.89.59:9092 --topic bi_logs --partition 11 --groupId bigdata --folder /root/liang/kafka/
[root@emr-header-1 kafka]# jps -v | grep 4583
4583 KafkaConsumeJob -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Dlog4j.configuration=file:./log4j.properties -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Xms512m -Xmx512m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m
