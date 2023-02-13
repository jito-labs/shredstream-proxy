#!/bin/bash
set -eu

# get busybox here: https://busybox.net/downloads/binaries/

echo "Rerun this script everytime shredstream-proxy is killed, as busybox nc will ignore subsequent connections"

# kill any previous instances
pkill busybox || true

# start new listeners
nohup ./busybox nc -v -u -l -p 9900 > 9900.out &
nohup ./busybox nc -v -u -l -p 9901 > 9901.out &
nohup ./busybox nc -v -u -l -p 9902 > 9902.out &

# let socket show up
sleep 0.5
sudo ss --all -upn | grep 990
