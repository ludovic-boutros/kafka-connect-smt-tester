package bzh.lboutros.tester;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public abstract class SMTPipelineTestBase<T extends ConnectRecord<T>> {

    private SMTPipelineTester<T> tester;

    public SMTPipelineTestBase() {
        super();
    }

    protected abstract String getConnectorConfigurationFilename();

    @BeforeEach
    public void beforeEach() throws IOException {
        tester = new SMTPipelineTester<>(getConnectorConfigurationFilename());
    }

    @AfterEach
    public void teardown() throws Exception {
        tester.close();
    }

    protected Result transformDataFromFile(String inputTopic, String inputFilename, ConnectUtils.RecordSupplier<T> supplier) throws IOException {
        return tester.transformDataFromFile(inputTopic, inputFilename, supplier);
    }
}
