@echo off
cd C:\kafka-2.8.1\bin\windows
kafka-server-start.bat ..\..\config\server.properties
pause