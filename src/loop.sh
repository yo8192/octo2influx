#!/bin/bash

function usage {
	echo "FREQ={seconds or m, h, d suffix} $0"
	echo "  FREQ: how often to get the data from Octopus, in seconds, or with a suffix 'm' for minutes, 'h' for hours or 'd' for days."
	exit 1
}

if ! [[ "$FREQ" =~ ^[0-9]+[smhd]?$ ]]; then
       echo "Invalid freq '$FREQ'"
       usage
fi

echo "$(date +"%F %T") Starting with FREQ=$FREQ..."
while : ; do
	# Arbitrary sleep to make sure we don't DDoS Octopus if we have a bug
	echo "$(date +"%F %T") Sleeping short amount of time out of safety..."
	sleep 7

	python3 ./octo2influx.py

	echo "$(date +"%F %T") Sleeping $FREQ..."
	sleep "$FREQ"
done
