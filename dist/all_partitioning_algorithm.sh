#!/bin/bash
arguments=""
for var in "$@"
do
    arguments="$arguments $var"
done
java -Xmx32G -jar GASGD.jar $arguments -partitioning_algorithm hdrf
java -Xmx32G -jar GASGD.jar $arguments -partitioning_algorithm greedy
java -Xmx32G -jar GASGD.jar $arguments -partitioning_algorithm hashing
java -Xmx32G -jar GASGD.jar $arguments -partitioning_algorithm grid
java -Xmx32G -jar GASGD.jar $arguments -partitioning_algorithm pds