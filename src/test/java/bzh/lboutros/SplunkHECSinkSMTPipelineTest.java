package bzh.lboutros;

import bzh.lboutros.tester.SMTPipelineTestBase;
import bzh.lboutros.tester.record.Record;
import bzh.lboutros.tester.record.SinkRecordSupplier;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static bzh.lboutros.tester.ConnectUtils.getRecordFromFile;
import static bzh.lboutros.tester.ConnectUtils.getResultRecordAsNormalizedPrettyString;

public class SplunkHECSinkSMTPipelineTest extends SMTPipelineTestBase<SinkRecord> {
    @Override
    protected String getConnectorConfigurationFilename() {
        return "splunk-hec-sink-connector.json";
    }

    @Test
    public void testInternalServerEvent() throws IOException {
        // Given
        String expectedOutput = getResultRecordAsNormalizedPrettyString(
                getRecordFromFile("internal-server-splunk-input-event.json"));
        SinkRecordSupplier recordSupplier = new SinkRecordSupplier("internal-server-output-event.json");

        // When
        Record record = super.transformDataFromFile(recordSupplier);

        // Then
        Assertions.assertEquals(expectedOutput, getResultRecordAsNormalizedPrettyString(record));
    }
}
