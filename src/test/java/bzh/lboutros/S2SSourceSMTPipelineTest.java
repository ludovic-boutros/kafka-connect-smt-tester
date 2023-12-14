package bzh.lboutros;

import bzh.lboutros.tester.ConnectUtils.SourceRecordSupplier;
import bzh.lboutros.tester.Result;
import bzh.lboutros.tester.SMTPipelineTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static bzh.lboutros.tester.ConnectUtils.getJsonFileAsNormalizedPrettyString;


public class S2SSourceSMTPipelineTest extends SMTPipelineTestBase<SourceRecord> {
    @Override
    protected String getConnectorConfigurationFilename() {
        return "splunk-s2s-source-connector.json";
    }

    @Test
    public void testInternalServerEvent() throws IOException {
        // Given
        String inputFilename = "internal-server-input-event.json";
        String inputTopic = "_internal_servers";
        String expectedOutput = getJsonFileAsNormalizedPrettyString("internal-server-ouput-event.json");

        String expectedTopic = "splunk._internal_servers";

        // When
        Result result = super.transformDataFromFile(inputTopic, inputFilename, new SourceRecordSupplier());

        // Then
        Assertions.assertEquals(expectedTopic, result.getTopic());
        Assertions.assertEquals(expectedOutput, result.getValue());
    }
}
