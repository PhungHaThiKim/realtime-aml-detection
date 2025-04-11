#!/bin/bash

if [ ! -d "/hadoop/dfs/name/current" ]; then
  echo "📦 First time format..."
  hdfs namenode -format -force
fi

hdfs --daemon start namenode
hdfs --daemon start datanode

tail -f /dev/null