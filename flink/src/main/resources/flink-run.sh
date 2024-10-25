#!/bin/bash

####################################
#                                  #
#             variable             #
#                                  #
####################################
# 类名
export className=FlinkJob
# 配置文件名 config.yml or repair.yml
export configName=config.yml

####################################
#                                  #
#              ignore              #
#                                  #
####################################
# flink env
export FLINK_HOME=/data/liang/flink-1.17.1
export FLINK_CONF_DIR=/data/liang/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`
# checkpoint dir
export folderName=$(echo ${className} | sed -E 's/([A-Z])/-\1/g' | sed -E 's/^-//g' | tr 'A-Z' 'a-z')
# yarn application name
export jobName=$(if [[ ${configName} == config* ]] ; then echo ${className} ; else echo ${className}RepairTest ; fi)
# restore dir
export restoreDir=$(if [[ $1 == hdfs* ]] ; then echo '-s '$1 ; else echo '' ; fi)

####################################
#                                  #
#    maybe change Memory or Slot   #
#                                  #
####################################
#  -D taskmanager.memory.network.fraction=0.5 \
#  -D taskmanager.memory.network.min=512m \
#  -D taskmanager.memory.network.max=512m \

/data/liang/flink-1.17.1/bin/flink run-application -t yarn-application ${restoreDir} \
  -D jobmanager.memory.process.size=2g \
  -D taskmanager.memory.process.size=5g \
  -D taskmanager.numberOfTaskSlots=8 \
  -D state.checkpoints.dir=hdfs:///liang/flink-checkpoints/${folderName} \
  -D yarn.ship-files=${configName} \
  -D yarn.application.name=${jobName} \
  -c com.liang.flink.job.${className} flink-1.0.jar ${configName}
