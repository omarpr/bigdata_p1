#!/bin/bash

END=6

for i in $(seq 1 $END)
do 
   sh regen.sh $i
done
