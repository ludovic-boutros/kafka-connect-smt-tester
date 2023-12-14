package bzh.lboutros;

import bzh.lboutros.utils.ConnectUtils;
import bzh.lboutros.utils.SMTPipelineTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static bzh.lboutros.utils.ConnectUtils.getJsonFileAsNormalizedPrettyString;


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
        SourceRecord result = super.transformDataFromFile(
                new ConnectUtils.SourceRecordSupplier(inputTopic, inputFilename, super.getConverter()));

        String resultTopic = result.topic();
        String output = super.getResultOutputEventAsNormalizedPrettyString(result);

        // Then
        Assertions.assertEquals(expectedTopic, resultTopic);
        Assertions.assertEquals(expectedOutput, output);
    }
}
