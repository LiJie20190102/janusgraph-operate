#!/bin/bash

CONF_DIR=$(dirname $0)
CONF_DIR=$(
  cd "$CONF_DIR"
  pwd
)

SERVICE_HOME=$(
  cd "$CONF_DIR/.."
  pwd
)

SERVER_NAME=janusgraph-operate

LOG_FILE=janusgraph-operate.log

SERVICE_LOG_DIR=${SERVICE_HOME}/log

if [ ! -d "$SERVICE_LOG_DIR" ]; then
    # 目录不存在，创建它
    mkdir -p "$SERVICE_LOG_DIR"
fi

MAIN_JAR="${SERVICE_HOME}/janusgraph-operate-query.jar"

MAIN_CLASS="com.qsdi.bigdata.janusgaph.ops.query.QueryData"

GC_OPTS="-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${SERVICE_LOG_DIR}/service-gc_%t.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=128m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${SERVICE_LOG_DIR}/service-dump.hprof"

JAVA_OPTS="-Duser.timezone=UTC+8 -Xms4g -Xmx4g -Xmn1024m -Dbolt.netty.buffer.low.watermark=32768 -Dbolt.netty.buffer.high.watermark=327680 $GC_OPTS"

CLASS_PATH=${SERVICE_HOME}/jars/*:${SERVICE_HOME}/conf:${MAIN_JAR}

export JAVA_HOME=${JAVA_HOME:-/usr}
export SERVICE_HOME=$SERVICE_HOME



get_server_pid(){
  echo $(ps -ef | grep "java" | grep "$SERVICE_HOME" | grep "$MAIN_CLASS" | grep -v "grep" |awk '{print $2}')
}
