#!/bin/bash


for i in $(seq 8); do for j in $(seq 8); do sudo uhubctl -l "$i-$j" 2> /dev/null && echo -e "$i-$j CHECK\n\n"; done; done
