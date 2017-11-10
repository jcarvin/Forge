#!/usr/bin/env bash
echo "running setup.sh"
sudo yum install docker -y
sudo service docker start

echo "Finished... "