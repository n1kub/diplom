@echo off
cd C:\kafka-2.8.1\bin\windows
zookeeper-server-start.bat ..\..\config\zookeeper.properties
pause