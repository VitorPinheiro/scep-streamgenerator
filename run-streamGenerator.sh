#!/bin/bash

printf "Starting TweetDB RDF stream generator...\n"
nohup java -jar stream-generator-v1.jar &
printf "TweetDB RDF stream started!\n"

