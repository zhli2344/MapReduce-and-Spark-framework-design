#!/bin/bash

#hdfs dfs -rm -r outfile
ant -f build.xml
hadoop jar CategoryTrend.jar CategoryTrend.Q1Driver AllVideos_short.csv outfile
hdfs dfs -get  outfile/part-r-00000 part-00000.txt
hdfs dfs -get  outfile/part-r-00001 part-00001.txt
hdfs dfs -get  outfile/part-r-00002 part-00002.txt
hdfs dfs -cat outfile/*
