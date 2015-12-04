#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

export HADOOP_HOME=/usr/hdp/current/hadoop-client
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_HOME=/usr/hdp/current/hadoop-yarn-client
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export SPARK_HOME=/usr/hdp/current/spark-client
export SPARK_CONF_DIR=/etc/spark/conf

source "$HADOOP_CONF_DIR"/hadoop-env.sh
source "$YARN_CONF_DIR"/yarn-env.sh
source "$SPARK_HOME"/bin/load-spark-env.sh

export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export JAVA_OPTIONS="$JAVA_OPTIONS -Djava.library.path=/usr/hdp/2.3.2.0-2950/hadoop/lib/native -Dtinkerpop.ext=ext -Dlog4j.configuration=conf/log4j-console.properties -Dhdp.version=2.3.2.0-2950"
GREMLINHOME=/nfs/home/ligniem/lib/apache-gremlin-console-3.1.0-incubating
export HADOOP_GREMLIN_LIBS=$GREMLINHOME/ext/hadoop-gremlin/plugin:$GREMLINHOME/ext/spark-gremlin/plugin:$GREMLINHOME/lib
export CLASSPATH=/etc/hadoop/conf:/etc/spark/conf:$GREMLINHOME/lib/*:/usr/hdp/current/spark-client/lib/*

cd $GREMLINHOME 
exec bin/gremlin.sh




