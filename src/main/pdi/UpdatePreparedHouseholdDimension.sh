#!/bin/sh
BASEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
echo "BASE DIR: ${BASEDIR}"
PENTAHO_DI_JAVA_OPTIONS="-Xms2048m -Xmx2548m -Xmn1536m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:MaxTenuringThreshold=1 -XX:SurvivorRatio=90 -XX:TargetSurvivorRatio=90 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -verbose:gc"

KETTLE_JNDI_ROOT="/opt/simple-jndi"

env PENTAHO_DI_JAVA_OPTIONS="${PENTAHO_DI_JAVA_OPTIONS}" /opt/pentaho/data-integration/pan.sh -file:${BASEDIR}/UpdatePreparedHouseholdDimension.ktr -param:reportDate=$1  -param:reportFile=$2 -param:reporterKey=$3

