# 展示完整的类名
jps -l | grep 4583
4583 com.liang.common.launch.KafkaConsumeJob
# 展示传给main方法的参数
jps -m | grep 4583
4583 KafkaConsumeJob --bootStrapServers 172.17.89.57:9092,172.17.89.58:9092,172.17.89.59:9092 --topic bi_logs --partition 11 --groupId bigdata --folder /root/liang/kafka/
# 展示传给jvm的参数
jps -v | grep 4583
4583 KafkaConsumeJob -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Dlog4j.configuration=file:./log4j.properties -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Xms512m -Xmx512m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m

# 每 1000ms打印4583这个Java进程的各分代使用百分比和gc情况
jstat -gcutil 4583 1000
# *C 代表当前分配额度(kb), *U 代表当前使用额度(kb)
jstat -gc 4583 1000
# NGCMN(New Generation 最小容量) NGCMX(New Generation 最大容量) NGC(New Generation 当前分配容量)
# Metaspace的Min和Max显示的值不准, 忽略不计
jstat -gccapacity 4583 1000

# 打印 JVM 信息
jmap -heap 4583
# 打印 JVM 内部存活着的所有对象的数量和大小, 会引发一次Full GC
jmap -histo:live 4583

#玩不明白
jstack
