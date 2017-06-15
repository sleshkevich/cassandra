@echo off

set startTime=%time%
java -jar data_loader.jar localhost whia_ data/alpr-1300
echo Start time: %startTime%
echo End time: %time%
