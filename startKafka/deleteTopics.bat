@echo off
cd C:\kafka-2.8.1\bin\windows
kafka-topics.bat --list --bootstrap-server localhost:9092
kafka-topics --bootstrap-server localhost:9092 --topic %1 --delete
kafka-topics.bat --list --bootstrap-server localhost:9092
pause