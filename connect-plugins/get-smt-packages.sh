#! /bin/bash

BASEDIR=$(dirname $0)
echo "$BASEDIR"

confluent-hub install confluentinc/connect-transforms:1.4.4 --component-dir "$BASEDIR" --no-prompt
confluent-hub install jcustenborder/kafka-connect-transform-common:0.1.0.58  --component-dir "$BASEDIR" --no-prompt
curl -L -O https://github.com/ludovic-boutros/kafka-custom-transforms/releases/download/v1.0/lboutros-kafka-custom-transforms-1.0.zip --output-dir "$BASEDIR"

find "$BASEDIR" -name '*.zip' -exec unzip -d "$BASEDIR" {} \;