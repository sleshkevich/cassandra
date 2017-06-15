#!/bin/bash

startTime=`date`
java -jar data_loader.jar localhost whia_ data/alpr-1300
echo Start Time: $startTime
echo Finish Time: `date`
