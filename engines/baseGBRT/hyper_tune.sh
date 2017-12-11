#!/bin/bash

printf "Usage: ./hyper_tune.sh <mode> <engine-id> <metric-class> \n"
printf "mode is either: \n all: build, register, train, evaluate \n te:  train, evaluate \n e:   evaluate \n"

echo $2 > engineId

if [ "$1" == all ]; then
	printf "...........BUILDING...........\n"
	pio build --clean
	printf "...........REGISTERING...........\n"
	pio register --engine-id $2 --base-engine-id baseGBRT --base-engine-url ./
fi
if [ "$1" == all ] || [ "$1" == te ]; then
	printf "...........TRAINING...........\n"
	pio train --engine-id $2 --base-engine-url ./ --base-engine-id baseGBRT
fi
if [ "$1" == all ] || [ "$1" == te ] || [ "$1" == e ]; then
	printf "...........EVALUATING...........\n"
	pio eval com.laserfiche.$3 com.laserfiche.EngineParamsList --engine-id $2 --base-engine-url ./ --base-engine-id baseGBRT
fi

rm engineId