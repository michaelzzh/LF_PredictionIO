#This script is for building and training base engines, 
declare -A engines
engines["baseClassification"]=8080
engines["baseLinearReg"]=9000
engines["baseLogistic"]=9090

for engine in ${!engines[@]}
do
	(cd $PIO_ROOT/engines/$engine; pio build --verbose)
	(cd $PIO_ROOT/engines/$engine; pio register --engine-id $engine \
		--base-engine-url $PIO_ROOT/engines/$engine \
		--variant $PIO_ROOT/engines/$engine/engine.json\
	        --deploy-port ${engines[$engine]})
	pio train --engine-id $engine \
		--base-engine-url $PIO_ROOT/engines/$engine \
		--base-engine-id $engine \
		--variant $PIO_ROOT/engines/$engine/engine.json \
		--pre-deployment
done
