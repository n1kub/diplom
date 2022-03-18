@echo off
start runZookeeper.bat
ping -n 10 127.0.0.1 >NUL
start runBroker.bat
if [%2] EQU [] (
    exit
)
if [%2] NEQ [] (
    if [%1] NEQ newtopic (
        echo "incorrect call"
        pause
        exit
    )
)
ping -n 10 127.0.0.1 >NUL
start createTopics.bat %2
