#!/bin/bash

# ssh -A -L 8888:localhost:8000 -R 2222:localhost:22 -i ~/.ssh/ec2.pem ubuntu@[ec2server]
# wget https://raw.githubusercontent.com/mrgriscom/promviz/master/script/ec2setup.sh ; chmod u+x *.sh ; ./ec2setup.sh

sudo apt-get update
sudo apt-get install git emacs python-pip python-dev openjdk-7-jdk unzip
sudo pip install requests tornado
git clone https://github.com/mrgriscom/promviz.git

cd /tmp
wget https://bitbucket.org/jraedler/polygon2/downloads/Polygon2-2.0.7.zip
unzip Polygon2-2.0.7.zip
cd Polygon2-2.0.7/
sudo python setup.py install

# make ssd partition
echo 'n p [enter] [enter] [enter] w'
sudo fdisk /dev/xvdb
sudo mkfs -t ext4 /dev/xvdb1
sudo mkdir /mnt/ssd
sudo mount /dev/xvdb1 /mnt/ssd
sudo chown ubuntu:ubuntu /mnt/ssd

bash ~/promviz/script/ec2pull.sh
