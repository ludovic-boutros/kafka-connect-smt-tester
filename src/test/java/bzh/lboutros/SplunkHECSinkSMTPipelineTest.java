package bzh.lboutros;

import bzh.lboutros.utils.ConnectUtils;
import bzh.lboutros.utils.SMTPipelineTestBase;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static bzh.lboutros.utils.ConnectUtils.getJsonFileAsNormalizedPrettyString;

public class SplunkHECSinkSMTPipelineTest extends SMTPipelineTestBase<SinkRecord> {
    @Override
    protected String getConnectorConfigurationFilename() {
        return "splunk-hec-sink-connector.json";
    }

    @Test
    public void testInternalServerEvent() throws IOException {
        // Given
        String inputFilename = "internal-server-ouput-event.json";
        String inputTopic = "splunk._internal_servers";
        String expectedOutput = getJsonFileAsNormalizedPrettyString("internal-server-splunk-input-event.json");

        // When
        SinkRecord result = transformDataFromFile(
                new ConnectUtils.SinkRecordSupplier(inputTopic, inputFilename, super.getConverter()));

        String output = getResultOutputEventAsNormalizedPrettyString(result);

        // Then
        Assertions.assertEquals(expectedOutput, output);
    }
}
