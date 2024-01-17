package bzh.lboutros;

import bzh.lboutros.tester.SMTPipelineTestBase;
import bzh.lboutros.tester.record.Record;
import bzh.lboutros.tester.record.SourceRecordSupplier;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static bzh.lboutros.tester.ConnectUtils.getRecordFromFile;
import static bzh.lboutros.tester.ConnectUtils.getResultRecordAsNormalizedPrettyString;


public class S2SSourceSMTPipelineTest extends SMTPipelineTestBase<SourceRecord> {
    @Override
    protected String getConnectorConfigurationFilename() {
        return "splunk-s2s-source-connector.json";
    }

    @Test
    public void testInternalServerEvent() throws IOException {
        // Given
        String expectedOutput = getResultRecordAsNormalizedPrettyString(
                getRecordFromFile("internal-server-output-event.json"));
        SourceRecordSupplier recordSupplier = new SourceRecordSupplier("internal-server-input-event.json");

        // When
        Record record = super.transformDataFromFile(recordSupplier);

        // Then
        Assertions.assertEquals(expectedOutput, getResultRecordAsNormalizedPrettyString(record));
    }
}
