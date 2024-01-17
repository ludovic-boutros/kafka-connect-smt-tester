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