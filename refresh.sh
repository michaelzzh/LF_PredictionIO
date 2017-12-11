kill $(lsof -t -i:7070)

rm -r PredictionIO-0.10.0-incubating

rm PredictionIO-0.10.0-incubating.tar.gz

./make-distribution.sh

tar zxvf PredictionIO-0.10.0-incubating.tar.gz

mkdir PredictionIO-0.10.0-incubating/vendors

tar zxvfC spark-1.5.1-bin-hadoop2.6.tgz PredictionIO-0.10.0-incubating/vendors

./build-base-engines.sh

pio eventserver &