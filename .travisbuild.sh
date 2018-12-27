#!/bin/bash

# build using Maven
mvn clean install 2>&1 | grep -v "Downloading" | grep -v "Downloaded"
