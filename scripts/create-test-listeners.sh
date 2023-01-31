#!/bin/bash

set -eu

# kill any previous instances
pkill busybox

# start new listeners
nohup ./busybox nc -v -u -l -p 9900 > 9900 &
nohup ./busybox nc -v -u -l -p 9901 > 9901 &
nohup ./busybox nc -v -u -l -p 9902 > 9902 &

# let socket show up
sleep 0.5
sudo ss --all -upn | grep 990
