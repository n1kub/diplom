@echo off
cd C:\kafka-2.8.1\bin\windows
kafka-topics.bat --create --topic %1 --bootstrap-server localhost:9092
kafka-topics.bat --list --bootstrap-server localhost:9092
pause