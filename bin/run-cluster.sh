#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage : $0 <FLUO HOME> <PHRASECOUNT_HOME> <TXT FILES DIR>"
  exit 
fi

FLUO_HOME=$1
PC_HOME=$2
#set the following to a directory containg text files
TXT_DIR=$3

#derived variables
APP_PROPS=$FLUO_HOME/apps/phrasecount/conf/fluo.properties
PC_JAR=$PC_HOME/target/phrasecount-0.0.1-SNAPSHOT.jar
#TODO should not be pulling log4j jars from FLUO_HOME/lib
export CLASSPATH=$PC_JAR:`$FLUO_HOME/bin/mini-fluo classpath -a -z -H`:$FLUO_HOME/lib/log4j/*

if [ ! -f $FLUO_HOME/conf/fluo.properties ]; then
  echo "Fluo is not configured, exiting."
  exit 1
fi

#create new application dir
$FLUO_HOME/bin/fluo new phrasecount

#build and copy phrasecount jar
(cd $PC_HOME; mvn package -DskipTests)

FLUO_APP_LIB=$FLUO_HOME/apps/phrasecount/lib/
cp $PC_JAR $FLUO_APP_LIB
DEP_PLUGIN=org.apache.maven.plugins:maven-dependency-plugin:2.10
mvn $DEP_PLUGIN:get -Dartifact=io.fluo:fluo-recipes-core:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB
mvn $DEP_PLUGIN:get -Dartifact=io.fluo:fluo-recipes-accumulo:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB
# Add kryo and its dependencies
mvn $DEP_PLUGIN:get -Dartifact=com.esotericsoftware:kryo:3.0.3:jar -Ddest=$FLUO_APP_LIB
mvn $DEP_PLUGIN:get -Dartifact=com.esotericsoftware:minlog:1.3.0:jar -Ddest=$FLUO_APP_LIB
mvn $DEP_PLUGIN:get -Dartifact=com.esotericsoftware:reflectasm:1.10.1:jar -Ddest=$FLUO_APP_LIB
mvn $DEP_PLUGIN:get -Dartifact=org.objenesis:objenesis:2.1:jar -Ddest=$FLUO_APP_LIB

export CLASSPATH=$FLUO_APP_LIB/*:`$FLUO_HOME/bin/mini-fluo classpath -a -z -H`:$FLUO_HOME/lib/log4j/*

#TODO maybe have two command to make it easier to understand.... one to create table and one print observer config
java phrasecount.cmd.Setup $APP_PROPS pcExport >> $APP_PROPS

$FLUO_HOME/bin/fluo init phrasecount -f
$FLUO_HOME/bin/fluo start phrasecount
$FLUO_HOME/bin/fluo info phrasecount

#Load data
java phrasecount.cmd.Load $APP_PROPS $TXT_DIR

#wait for all notifications to be processed.
$FLUO_HOME/bin/fluo wait phrasecount

#print phrase counts
#java phrasecount.cmd.Print $APP_PROPS 

#compare phrasecounts in Fluo table and export table 
#java phrasecount.cmd.Compare $APP_PROPS phrasecount pcExport

$FLUO_HOME/bin/fluo stop phrasecount

