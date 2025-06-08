#!/bin/bash


mkdir -p logs

for i in $(seq 1 1); do
    python scripts/message_test.py > ./logs/test$i.log &
done

wait