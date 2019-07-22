#!/bin/bash

spark-submit \
    --master local[4] \
    part2.py \
    --input $1 \
    --output $2 \
	 

    
