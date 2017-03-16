#This script is for building and training base engines, 
for enginePath in $PIO_ROOT/engines/*/
do
	engine=$(basename $enginePath)
	(cd $PIO_ROOT/engines/$engine; pio build --verbose)
	(cd $PIO_ROOT/engines/$engine; pio register --engine-id $engine \
		--base-engine-url $PIO_ROOT/engines/$engine \
		--variant $PIO_ROOT/engines/$engine/engine.json)
	pio train --engine-id $engine \
		--base-engine-url $PIO_ROOT/engines/$engine \
		--base-engine-id $engine \
		--variant $PIO_ROOT/engines/$engine/engine.json \
		--pre-deployment
done