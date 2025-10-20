#!/bin/bash
sysctl net.inet.udp.maxdgram
sudo sysctl -w net.inet.udp.maxdgram=65535
sysctl net.inet.udp.maxdgram
