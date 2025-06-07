#!/bin/bash


for i in $(seq 1 15); do
    python scripts/message_test.py > test$i.log &
done

wait