#!/bin/sh
# gen.sh <table> <n> <base_ts> <step_ns> <host>
TBL=$1; N=$2; BASE=$3; STEP=$4; H=$5
i=1
while [ $i -le $N ]; do
  TS=$(( BASE + i * STEP ))
  MV=$(( i % 100 ))
  echo "$TBL,host=$H v=${MV}.5 $TS"
  i=$((i+1))
done
