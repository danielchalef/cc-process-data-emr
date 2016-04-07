#!/bin/bash
# Expects cluster id as first argument
# and list of files to process as second

PYTHON=/usr/bin/python2.7

UUID=$(cat /proc/sys/kernel/random/uuid)

echo "Starting job $UUID on cluster $1 with data file $2"

cat $2 | $PYTHON cc-process-emr.py -r emr \
	--conf-path mrjob.conf \
	--no-output \
	--output-dir=s3://dc-analytics/$UUID/ \
	--cluster-id=$1 
