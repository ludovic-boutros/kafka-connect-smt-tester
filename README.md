# The project

Testing Kafka Connect transforms is not easy. This project aims to provide a simple way to test Kafka Connect
transforms.
You can use it as a CLI or as a library for unit testing.

# Prerequisites

First you need to unzip kafka connect transforms in the `connect-plugins` directory.

The needed transform packages for unit tests are:

- confluent connect transformations: https://www.confluent.io/hub/confluentinc/connect-transforms
- jcustenborder common
  transformations: https://www.confluent.io/hub/jcustenborder/kafka-connect-transform-common (https://github.com/jcustenborder/kafka-connect-transform-common)
- my extended Hoist
  transformation: https://github.com/ludovic-boutros/kafka-custom-transforms/releases/download/v1.0/lboutros-kafka-custom-transforms-1.0.zip (https://github.com/ludovic-boutros/kafka-custom-transforms)

A simple script is provided to download and unzip the needed
transforms: [get-smt-packages.sh](connect-plugins/get-smt-packages.sh).
This script uses the confluent-hub script. You can download it
here: https://docs.confluent.io/current/connect/managing/confluent-hub/client.html#confluent-hub-client.

# Usage

## CLI

Example:

```shell
./smt-test -c src/test/resources/splunk-s2s-source-connector.json -p connect-plugins -i src/test/resources/internal-server-input-event.json --type source
```

The general CLI usage is:

```shell
Missing required options: '--connector-config=<connectorConfigFile>', '--plugin-path=<pluginPath>', '--input-event=<inputEventFile>'
Usage: test [-hV] -c=<connectorConfigFile> -i=<inputEventFile> -p=<pluginPath>
            [--type=<type>]
Tests a Kafka Connect Connector SMT pipeline with a given event input.
  -c, --connector-config=<connectorConfigFile>
                      The Connector configuration file.
  -h, --help          Show this help message and exit.
  -i, --input-event=<inputEventFile>
                      The event input file.
  -p, --plugin-path=<pluginPath>
                      The Connector needed plugin path.
      --type=<type>   source or sink connector. Default: source.
  -V, --version       Print version information and exit.
```

## Usage for unit testing

```shell
mvn test
```