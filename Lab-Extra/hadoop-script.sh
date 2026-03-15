#!/bin/sh
#script commands

echo "run StationTemp in pseudo-distributed mode"
cd ~/workspace/StationTemp
hadoop fs -rm -r input
hadoop fs -mkdir -p input
hadoop fs -put NCDC-Weather.txt input
hadoop jar /home/cloudera/Desktop/StationTemp.jar input output

#exit
#col -b < commands > commmands.txt

