#!/bin/bash

cd ~/promviz/promviz/
scp -r -P 2222 drew@localhost:/home/drew/dev/promviz/promviz/bin .
cp bin/config.properties.ec2 bin/config.properties
