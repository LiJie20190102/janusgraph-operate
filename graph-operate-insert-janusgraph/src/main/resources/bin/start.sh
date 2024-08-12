#!/bin/bash


BIN_DIR=$(dirname $0)
BIN_DIR=$(
  cd "$BIN_DIR"
  pwd
)

source ${BIN_DIR}/service-env.sh



function start()
{
  server_pid=$(get_server_pid)

  if [ -n "$server_pid" ]; then
      echo "ERROR: The $SERVER_NAME already started!"
      echo "PID: $server_pid"
      exit 1
  fi
  
  echo "starting es ops Service, logging to ${SERVICE_LOG_DIR}/${LOG_FILE}"
  echo "${JAVA_HOME}/bin/java  ${JAVA_OPTS} -cp ${CLASS_PATH} ${MAIN_CLASS}"
  nohup ${JAVA_HOME}/bin/java  ${JAVA_OPTS} -cp ${CLASS_PATH} ${MAIN_CLASS} >> ${SERVICE_LOG_DIR}/${LOG_FILE}  2>&1 &

  echo "end to start es ops Service"
}

start
