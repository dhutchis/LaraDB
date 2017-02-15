#!/usr/bin/env bash

for n in `seq 1 4`; do
  d=$(printf '2017-01-%02d' ${n})
  # Don't download if already there
  mkdir -p input
  f="input/bee-uw-v2dec-${d}.txt"
  [ -f "${f}" ] || wget -O "${f}" "http://beehive1.mcs.anl.gov/api/1/nodes/0000001e0610ba37/export?date=${d}&version=2"
  f="input/bee-denver-v2dec-${d}.txt"
  [ -f "${f}" ] || wget -O "${f}" "http://beehive1.mcs.anl.gov/api/1/nodes/0000001e0610ba72/export?date=${d}&version=2"
done
