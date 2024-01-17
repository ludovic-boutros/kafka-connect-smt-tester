package bzh.lboutros;

import bzh.lboutros.tester.ConnectUtils;
import bzh.lboutros.tester.SMTPipelineTester;
import bzh.lboutros.tester.record.Record;
import bzh.lboutros.tester.record.SinkRecordSupplier;
import bzh.lboutros.tester.record.SourceRecordSupplier;
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

    @CommandLine.Option(names = {"--type"}, description = "source or sink connector. Default: source.")
    private String type = "source";

    @Override
    public Integer call() throws Exception {
        Record record;
        if (type.equals("source")) {
            try (SMTPipelineTester<SourceRecord> tester = new SMTPipelineTester<>(pluginPath, connectorConfigFile)) {
                SourceRecordSupplier recordSupplier = new SourceRecordSupplier(inputEventFile);
                record = tester.transformDataFromFile(recordSupplier);
            }
        } else if (type.equals("sink")) {
            try (SMTPipelineTester<SinkRecord> tester = new SMTPipelineTester<>(pluginPath, connectorConfigFile)) {
                SinkRecordSupplier recordSupplier = new SinkRecordSupplier(inputEventFile);
                record = tester.transformDataFromFile(recordSupplier);
            }
        } else {
            System.err.println("Invalid type: " + type);
            return 1;
        }
        Record recordFromFile = ConnectUtils.getRecordFromFile(inputEventFile);
        System.out.println("Input topic: " + recordFromFile.getTopic());
        System.out.println("Input headers:\n" + recordFromFile.getHeaders());
        System.out.println("Input key:\n" + recordFromFile.getKey());
        System.out.println("Input event:\n" + recordFromFile.getValue());

        System.out.println("Result topic: " + record.getTopic());
        System.out.println("Result headers:\n" + record.getHeaders());
        System.out.println("Result key:\n" + record.getKey());
        System.out.println("Result event:\n" + record.getValue());
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}