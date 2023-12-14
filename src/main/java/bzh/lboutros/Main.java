package bzh.lboutros;

import bzh.lboutros.tester.ConnectUtils;
import bzh.lboutros.tester.Result;
import bzh.lboutros.tester.SMTPipelineTester;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "test", mixinStandardHelpOptions = true, version = "smt 1.0",
        description = "Tests a Kafka Connect Connector SMT pipeline with a given event input.")
public class Main implements Callable<Integer> {

    @CommandLine.Option(names = {"-c", "--connector-config"}, description = "The Connector configuration file.", required = true)
    private String connectorConfigFile;

    @CommandLine.Option(names = {"-p", "--plugin-path"}, description = "The Connector needed plugin path.", required = true)
    private String pluginPath;

    @CommandLine.Option(names = {"-i", "--input-event"}, description = "The event input file.", required = true)
    private String inputEventFile;

    @CommandLine.Option(names = {"-t", "--topic"}, description = "The event input/output topic.", required = true)
    private String topic;

    @CommandLine.Option(names = {"--type"}, description = "source or sink connector. Default: source.")
    private String type = "source";

    @Override
    public Integer call() throws Exception {
        Result result;
        if (type.equals("source")) {
            try (SMTPipelineTester<SourceRecord> tester = new SMTPipelineTester<>(pluginPath, connectorConfigFile)) {
                result = tester.transformDataFromFile(topic, inputEventFile, new ConnectUtils.SourceRecordSupplier());
            }
        } else if (type.equals("sink")) {
            try (SMTPipelineTester<SinkRecord> tester = new SMTPipelineTester<>(pluginPath, connectorConfigFile)) {
                result = tester.transformDataFromFile(topic, inputEventFile, new ConnectUtils.SinkRecordSupplier());

            }
        } else {
            System.err.println("Invalid type: " + type);
            return 1;
        }
        String inputEvent = ConnectUtils.getJsonFileAsNormalizedPrettyString(inputEventFile);
        System.out.println("Input topic: " + topic);
        System.out.println("Input event:\n" + inputEvent);
        System.out.println("Result topic: " + result.getTopic());
        System.out.println("Result event:\n" + result.getValue());

        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}