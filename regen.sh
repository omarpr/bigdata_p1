#!/bin/bash

INPUT_FILE=/home/omar/trump_tweets.txt
HDFS_OUTPUT=/home/omar/p1/
JARS_LOCATION=~/p1/
EXTRACT_LOCATION=~/p1/html/

if [ "$#" -ne 1 ]
then
   echo "Usage: ./regen.sh <exercise number>"
   exit 1
fi

hdfs dfs -rm -r ${HDFS_OUTPUT}ex$1
hadoop jar ${JARS_LOCATION}ex$1-1.0-SNAPSHOT.jar $INPUT_FILE ${HDFS_OUTPUT}ex$1
hdfs dfs -get ${HDFS_OUTPUT}ex$1/part-r-*
cat part-r-* > ${EXTRACT_LOCATION}ex${1}.txt
cat ${EXTRACT_LOCATION}ex${1}.txt | sort -t$'\t' -nr -k2 > ${EXTRACT_LOCATION}ex${1}.txt.sorted
mv -f ${EXTRACT_LOCATION}ex${1}.txt.sorted ${EXTRACT_LOCATION}ex${1}.txt
rm part-r-*
