#!/bin/sh
# gen_col.sh <table> <field_name> <n> <base_ts> <step_ns>
TBL=$1; FLD=$2; N=$3; BASE=$4; STEP=$5
i=1
while [ $i -le $N ]; do
  TS=$(( BASE + i * STEP ))
  MV=$(( i % 100 ))
  echo "$TBL $FLD=${MV}.5 $TS"
  i=$((i+1))
done
