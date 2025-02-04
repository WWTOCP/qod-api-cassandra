#!/bin/bash

# Define the target (change to your desired IP or hostname)
TARGET="8.8.8.8"

# Continuous ping with timestamps
while true; do
    echo -en "\e[32m$(date '+%Y-%m-%d %H:%M:%S') \e[0m- \e[31m$TARGET\e[0m "  # Print timestamp
    ping -4 -c 1 $TARGET | grep "time="         # Extract only relevant ping output
    sleep 1  # Adjust interval as needed (1 second delay)
done
