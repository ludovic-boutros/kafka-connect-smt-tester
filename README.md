# The project

Testing Kafka Connect transforms is not easy. This project aims to provide a simple way to test Kafka Connect
transforms.
You can use it as a CLI or as a library for unit testing.

# Prerequisites

First you need to unzip kafka connect transforms in the `connect-plugins` directory.

# Usage as CLI

```shell
./smt-test -c src/test/resources/splunk-s2s-source-connector.json -p connect-plugins -i src/test/resources/internal-server-input-event.json -t input-topic
```

# Usage for unit testing

```shell
mvn test
```